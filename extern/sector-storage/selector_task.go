package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type taskSelector struct {
	best []stores.StorageInfo //nolint: unused, structcheck
}

// 这个是C2的选择，这个函数返回的 taskSelector的结构体，这个结构体有下面3个方法：ok Cmp， 和默然加的 FindDataWoker
func newTaskSelector() *taskSelector {
	return &taskSelector{}
}

// 这里是定义的  type WorkerSelector interface 接口的实例

//
func (s *taskSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.workerRpc.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}

	// supported是true或者false
	_, supported := tasks[task]

	return supported, nil
}

func (s *taskSelector) Cmp(ctx context.Context, _ sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	atasks, err := a.workerRpc.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	btasks, err := b.workerRpc.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if len(atasks) != len(btasks) {
		return len(atasks) < len(btasks), nil // prefer workers which can do less
	}

	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &taskSelector{}

//这里是新加的，PL的代码没有
func (s *taskSelector) FindDataWoker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, spt abi.RegisteredSealProof, whnd *workerHandle) bool {

	// 先获取当前worker能干的任务类型，比如是p1， p2，还是c2； 返回的是s.taskTypes是 map[sealtasks.TaskType]struct{}的类型返回值
	//因为有的worker 可以做的任务有好几种，比如可以同时做p1，p2，c2.

	// func (s *schedTestWorker) TaskTypes(ctx context.Context) (map[sealtasks.TaskType]struct{}, error) {
	// 	return s.taskTypes, nil
	// }

	tasks, err := whnd.workerRpc.TaskTypes(ctx)
	if err != nil {
		return false
	}

	//比如 当前worker支持tasks[p1,p2]，现在传进来的任务要做的是P1；Map进来匹配到当前worker支持P1，supported就是true
	//如果传进来的任务是C2，Map进来匹配到当前worker不支持C2，supported就是false。 那么后面的 !supported 就生效，返回false
	if _, supported := tasks[task]; !supported {
		return false
	}

	//获取当前worker的路径，StoragePath，比如http：//xxxx，；可能这个worker挂载了好几个 storage path
	// func (s *schedTestWorker) Paths(ctx context.Context) ([]stores.StoragePath, error) {
	// 	return s.paths, nil
	// }
	//很多地方都用到这个path，下面的have 和for
	paths, err := whnd.workerRpc.Paths(ctx)
	if err != nil {
		return false
	}

	//这个paths，这里如果打印出来，就知道确切的内容了
	// workerRpc 是Worker类型
	/**
		type Worker interface {
			storiface.WorkerCalls

			TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

			// Returns paths accessible to the worker
			Paths(context.Context) ([]stores.StoragePath, error)

			Info(context.Context) (storiface.WorkerInfo, error)

			Session(context.Context) (uuid.UUID, error)

			Close() error // TODO: do we need this?
		}
	**/

	//创建一个map类型的空结构体，key是stores.ID类型
	have := map[stores.ID]struct{}{}

	// 覆盖stores.ID为 path.ID 这个key到have里面，对应把上述paths的路径都存到have这个结构体里面，
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	//从 taskSelector 的best里面进行循环；
	//[]stores.StorageInfo 里面有很多worker的存储信息，如
	// {
	// 	"StoragePaths": [
	// 	  {
	// 		"Path": "/mnt/lotusworker"
	// 	  }
	// 	]
	//   }root@p1-01:/mnt/lotusworker# cat sectorstore.json
	//   {
	// 	"ID": "d4149b99-db8c-4aa8-91ad-ee644eed5ae4",
	// 	"Weight": 10,
	// 	"CanSeal": true,
	// 	"CanStore": false

	//miner上面维护这一张所有worker的StorageInfo

	//判断是否是本worker封装目录下的数据
	//是判断这个worker是否拥有这个目录
	//多个worker可以指向相同目录的，比如P1 和p2 worker都指向这个storage.json都目录
	//当前选择的worker 如果 不拥有 这个目录的话，就得重新再匹配worker，直到匹配到拥有这个目录的worker。

	//目录限制了 不会远程fetch
	// s.best是p1刚刚完成的worker的目录,info.ID 是sectorstore.json里面的 d4149b99-db8c-4aa8-91ad-ee644eed5ae4
	// 上面的have[path.ID]是当前被匹配到的worker的 storage ID
	// 这两个ID必须一致，才会返回true

	for _, info := range s.best {
		if info.Weight != 0 { // 为0的权重是fecth来的，不是本地的
			//在have里面找 是否有这个worker的ID，不关心具体的id是多少。
			if _, ok := have[info.ID]; ok {
				return true
			}
		}
	}

	return false
}
