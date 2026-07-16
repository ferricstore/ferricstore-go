package ferricstore

import "sync"

func runBoundedTopologyTasks(limit int, tasks []func()) {
	if len(tasks) == 0 {
		return
	}
	if len(tasks) == 1 {
		tasks[0]()
		return
	}
	if limit <= 0 || limit > len(tasks) {
		limit = len(tasks)
	}
	jobs := make(chan func(), limit)
	var workers sync.WaitGroup
	workers.Add(limit)
	for range limit {
		go func() {
			defer workers.Done()
			for task := range jobs {
				task()
			}
		}()
	}
	for _, task := range tasks {
		jobs <- task
	}
	close(jobs)
	workers.Wait()
}
