package main

import (
	"context"
	"github.com/wandb/parallel"
)

func main() {
	g := parallel.Unlimited(context.Background())
	g.Go(bear)
	g.Wait()
}

func bear(_ context.Context) {
	g := parallel.Unlimited(context.Background())
	g.Go(foo)
	g.Wait()
}

func foo(_ context.Context) {
	bar()
}

func bar() {
	panic("baz")
}
