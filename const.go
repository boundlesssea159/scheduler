package scheduler

import "errors"

var (
	DuplicatedBatchTaskError = errors.New("提交重复批次任务")
	TokenTimeoutError        = errors.New("任务获取token超时")
	InvalidParamError        = errors.New("参数错误")
)
