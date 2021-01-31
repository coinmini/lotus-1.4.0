package sectorstorage

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
	FindDataWoker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, spt abi.RegisteredSealProof, a *workerHandle) bool
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
	workerOnFree   chan struct{}
	todo           []*workerRequest
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	todo          []*workerRequest
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

//应该是被外部调用的新的调度？
func newScheduler() *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

//被manager.go里面的
/**
err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, m.schedFetch(sector, storiface.FTCache|storiface.FTSealed, storiface.PathSealing, storiface.AcquireMove),
	func(ctx context.Context, w Worker) error {
		err := m.startWork(ctx, w, wk)(w.SealPreCommit2(ctx, sector, phase1Out))
		if err != nil {
			return err
		}

		waitRes()
		return nil
	})
	if err != nil {
		return storage.SectorCids{}, err
	}
**/

//scheduler的调度方法
func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	//这是个go routing，肯定是并发的过程；把任务放到了sh.schedule的管道里面，由sh.schedule进行下一步
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

// manger 调用这边的scheduler
func (sh *scheduler) runSched() {
	//函数结束时执行 关闭，
	defer close(sh.closed)
	//
	go func() {

		//这个for会一直循环执行，检查调度的任务列表里面有没有任务，3分钟？
		//没必要调，任务队列没任务不会执行的
		for {
			time.Sleep(time.Second * 180) // 3分钟执行一次
			sh.workersLk.Lock()
			if sh.schedQueue.Len() > 0 {
				sh.windowRequests <- &schedWindowRequest{}
			}
			sh.workersLk.Unlock()
		}
	}()

	//下面调度各种情况，是一个一直在循环的的监控
	for {
		select {
		//检查有没有新的worker增加或者减少，worker added / changed/freed resources
		case <-sh.workerChange:
			sh.trySched()

		//worker掉了
		case toDisable := <-sh.workerDisable:
			sh.workersLk.Lock()
			sh.workers[toDisable.wid].enabled = false
			sh.workersLk.Unlock()
			toDisable.done()

		//有任务来了，会把任务放到schedQueue里面。
		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			sh.trySched()

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}

		//如果有上面的go里的 windowRequests 有请求了
		case <-sh.windowRequests:
			sh.trySched()

		//？？
		case ireq := <-sh.info:
			ireq(sh.diag())

		//关闭调度
		case <-sh.closing:
			sh.schedClose()
			return
		}
	}
}

//建立需要分配的任务列表
func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	//获取worker的uuid
	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

// try是先循环一遍，看看空闲的worker，还没正式开始分配
func (sh *scheduler) trySched() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	// 第一层循环先遍历多少个任务，看看怎么分配
	//第二层循环针对每个任务匹配worker，会遍历一遍worker
	log.Debugf("trySched %d queued", sh.schedQueue.Len())
	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ { // 遍历任务列表

		//定义每一个task，比如task[1], taks[2], 是一个数组
		task := (*sh.schedQueue)[sqi]

		tried := 0

		// 这两个表格是这个循环的目的
		var acceptable []WorkerID
		var freetable []int
		best := 0
		localWorker := false

		//第二步匹配合适的worker，会遍历一遍worker，看看有多少worker合适
		for wid, worker := range sh.workers {
			//如果当次循环里面没有可用的worker，就跳出本次循环，循环体下面的代码不再执行；进入下一次循环
			if !worker.enabled {
				continue
			}

			// 检查当前任务的类型是什么,P1, P2 ,还是C2； 如果是其他任务如 AP，FIN等就不进去这个判断，直接去下面的 ok 判断
			// 再调用WorkerSelector select，查看当前worker是否合适，比如有的worke设置的 P1 =true， P2 =true，C2 =false；
			// FindDataWoker 确定当前的worker的能干的任务类型，是否符合当前分配的任务类型
			if task.taskType == sealtasks.TTPreCommit1 || task.taskType == sealtasks.TTPreCommit2 || task.taskType == sealtasks.TTCommit1 {
				if isExist := task.sel.FindDataWoker(task.ctx, task.taskType, task.sector.ID, task.sector.ProofType, worker); !isExist {
					continue
				}
			}

			//
			// 调用WorkerSelector，找到了合适的worker
			ok, err := task.sel.Ok(task.ctx, task.taskType, task.sector.ProofType, worker)
			if err != nil || !ok {
				continue
			}

			// getTaskFreeCount 返回当前worker 有几个当前任务可以安排，比如P1机器
			freecount := sh.getTaskFreeCount(wid, task.taskType)
			if freecount <= 0 {
				continue
			}

			//尝试的次数++
			tried++

			// 把当前循环内的worker的 空闲的数量加到一个表格内
			freetable = append(freetable, freecount)
			// 把当前循环内的worker的 ID加到一个表格内
			acceptable = append(acceptable, wid)

			//再确认一遍当前这个worker是否存在，确认存在，设置localWorker = true，跳出寻找worker的循环
			//contiune是跳出此次循环，进入下一个循环，直到循环结束；break是直接结束当前循环

			if isExist := task.sel.FindDataWoker(task.ctx, task.taskType, task.sector.ID, task.sector.ProofType, worker); isExist {
				//这里的localWorker的意思就是指 用本机上的P2 进行计算，而不是拉远程的
				localWorker = true
				break
			}
		}

		// 第一种情况处理：在一遍循环里面找到了这个worker

		// 如果acceptable表格内有 worker id ， 此时len(acceptable)  =1，
		//并且localWorker = true； best 的 -1 后变成0，wid := acceptable[0]

		//第二种情况处理：循环一遍结束了，
		// 找到了这个worker，此时len(acceptable)  =1；但是突然down机了或者其他问题，突然不符合；
		//localWorker是false：1 >0, best = 0 , wid := acceptable[0]
		//

		//第三种情况处理：所有的worker循环一遍完成了,也没有匹配到合适的worker id
		// freetable 里面 0 > max 不成立。 tried == 0。

		if len(acceptable) > 0 {
			if localWorker {
				best = len(acceptable) - 1
			} else {
				max := 0
				for i, v := range freetable {
					if v > max {
						max = v
						best = i
					}
				}
			}

			//确定当前worker ID 做当前任务

			wid := acceptable[best]
			whl := sh.workers[wid]
			log.Infof("worker %s will be do the %+v jobTask!", whl.info.Hostname, task.taskType)

			//从任务列表内把当前的任务序号去掉，
			sh.schedQueue.Remove(sqi)

			//总任务数--,永远保证从第一个任务开始
			sqi--

			//调用assignWorker 真正的分配任务，返回nil或者error
			if err := sh.assignWorker(wid, whl, task); err != nil {
				log.Error("assignWorker error: %+v", err)
				go task.respond(xerrors.Errorf("assignWorker error: %w", err))
			}
		}

		//如果上面的循环都没有找到worker，就提示没有符合现在要做的job的类型的worker
		if tried == 0 {
			log.Infof("no worker do the %+v jobTask!", task.taskType)
		}
	}
}

//下面的官方代码里面也有的 在 sched_worker.go里面

func (sh *scheduler) assignWorker(wid WorkerID, w *workerHandle, req *workerRequest) error {
	//为匹配的worker 增加run的任务
	sh.taskAddOne(wid, req.taskType)
	//把当前任务需要的资源，如果内存，cpu，显存准备好
	needRes := ResourceTable[req.taskType][req.sector.ProofType]

	w.lk.Lock()
	w.preparing.add(w.info.Resources, needRes)
	w.lk.Unlock()

	//这里是个并发，同时有多个fetch
	go func() {
		// first run the prepare step (e.g. fetching sector data from other worker)
		err := req.prepare(req.ctx, sh.workTracker.worker(wid, w.workerRpc)) // fetch扇区
		//开始fetch就锁当前worker
		sh.workersLk.Lock()

		if err != nil {
			sh.taskReduceOne(wid, req.taskType)
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()

			select {
			case w.workerOnFree <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		// wait (if needed) for resources in the 'active' window
		err = w.active.withResources(wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			select {
			case w.workerOnFree <- struct{}{}:
			case <-sh.closing:
			}

			// Do the work!
			err = req.work(req.ctx, sh.workTracker.worker(wid, w.workerRpc))
			//单前的worker 减掉一个刚刚完成的任务
			sh.taskReduceOne(wid, req.taskType)

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

// 调度关闭，清除调度里面worker信息
func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

//
func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sh *scheduler) taskAddOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.workers[wid]; ok {
		whl.info.TaskResourcesLk.Lock()
		defer whl.info.TaskResourcesLk.Unlock()
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			counts.RunCount++
		}
	}
}

func (sh *scheduler) taskReduceOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.workers[wid]; ok {
		whl.info.TaskResourcesLk.Lock()
		defer whl.info.TaskResourcesLk.Unlock()
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			counts.RunCount--
		}
	}
}

//检查worker能做的任务总数和当前在做的任务数量
func (sh *scheduler) getTaskCount(wid WorkerID, phaseTaskType sealtasks.TaskType, typeCount string) int {
	// 检查sh.workers是是否包含这个worker id
	//whl = workerHandler
	if whl, ok := sh.workers[wid]; ok {
		//看看当前worker针对当前的任务类型，总资源够做多少个
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			whl.info.TaskResourcesLk.Lock()
			defer whl.info.TaskResourcesLk.Unlock()

			// limit表示能做的总数
			if typeCount == "limit" {
				return counts.LimitCount
			}

			//run表示当前正在run的数量
			if typeCount == "run" {
				return counts.RunCount
			}
		}
	}
	return 0
}

//获取worker空闲的任务数量
func (sh *scheduler) getTaskFreeCount(wid WorkerID, phaseTaskType sealtasks.TaskType) int {
	limitCount := sh.getTaskCount(wid, phaseTaskType, "limit") // json文件限制的任务数量
	runCount := sh.getTaskCount(wid, phaseTaskType, "run")     // 运行中的任务数量

	//结果就是可用的资源数量
	freeCount := limitCount - runCount

	if limitCount == 0 { // 0:禁止
		return 0
	}

	whl := sh.workers[wid]
	log.Infof("worker %s %s: %d free count", whl.info.Hostname, phaseTaskType, freeCount)

	// 做P1任务或者AP的worker，有空闲数量就行。
	if phaseTaskType == sealtasks.TTAddPiece || phaseTaskType == sealtasks.TTPreCommit1 {
		if freeCount >= 0 { // 空闲数量不小于0，小于0也要校准为0
			return freeCount
		}
		return 0
	}

	// 做p2的任务的worker，要保证没有C2在做
	if phaseTaskType == sealtasks.TTPreCommit2 || phaseTaskType == sealtasks.TTCommit1 {
		c2runCount := sh.getTaskCount(wid, sealtasks.TTCommit2, "run")
		if freeCount >= 0 && c2runCount <= 0 { // 需做的任务空闲数量不小于0，且没有c2任务在运行
			return freeCount
		}
		log.Infof("worker already doing C2 taskjob")
		return 0
	}

	//做C2任务的worker，要保证没有P2，C1 在做
	if phaseTaskType == sealtasks.TTCommit2 {
		p2runCount := sh.getTaskCount(wid, sealtasks.TTPreCommit2, "run")
		c1runCount := sh.getTaskCount(wid, sealtasks.TTCommit1, "run")
		if freeCount >= 0 && p2runCount <= 0 && c1runCount <= 0 { // 需做的任务空闲数量不小于0，且没有p2\c1任务在运行
			return freeCount
		}
		log.Infof("worker already doing P2C1 taskjob")
		return 0
	}

	//
	if phaseTaskType == sealtasks.TTFetch || phaseTaskType == sealtasks.TTFinalize ||
		phaseTaskType == sealtasks.TTUnseal || phaseTaskType == sealtasks.TTReadUnsealed { // 不限制
		return 1
	}

	return 0
}
