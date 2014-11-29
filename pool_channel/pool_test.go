package pool_channel

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	no int32
)

type resource_symulator struct {
	id int32
}

func resourceNew() (r *resource_symulator, err error) {

	r = new(resource_symulator)
	r.id = atomic.AddInt32(&no, 1)
	time.Sleep(time.Microsecond * 1)
	return
}

func (r *resource_symulator) resourceDel() (err error) {
	r.id = 0
	time.Sleep(time.Microsecond * 1)
	return
}

func TestIntialize(t *testing.T) {

	var err error
	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p := NewPool(2, 5, create, destroy, test, nil)
	defer p.Close()

	msg, err := p.Get()
	if err != nil {
		t.Fatalf("Get Resource error: %s", err.Error())
	}

	if msg.Resource().(*resource_symulator).id != 1 {
		t.Fatalf("Resource id should be on = %d", msg)
	}
}

func TestBeyond(t *testing.T) {

	var err error
	create := func() (interface{}, error) {
		return resourceNew()
	}
	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p := NewPool(2, 5, create, destroy, test, nil)
	defer p.Close()

	if _, err = p.getAvailable(); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(); err == nil {
		t.Fatal("expected error on sixth get")
	}

	if err != ResourceExhaustedError {
		t.Fatalf("Error must be ResourceExhaustedError")
	}
}

//Test that we don't deadlock
func TestWait(t *testing.T) {

	var err error

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p := NewPool(2, 5, create, destroy, test, nil)
	defer p.Close()

	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	msg, err := p.Get()

	called := false
	go func() {
		msg.Close()
		called = true
	}()

	//this waits till msg.Close() is called in the go thread
	msg, err = p.Get()
	if err != nil {
		t.Fatal(err)
	}

	if !called {
		t.Fatal("Expected close of resource to block execution")
	}

}

func TestExcluse(t *testing.T) {

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	var min, max uint32
	min = 10
	max = 50

	no = 0
	p := NewPool(min, max, create, destroy, test, nil)
	defer p.Close()

	var waitgroup sync.WaitGroup
	check := make(map[int32]bool)
	var l sync.Mutex

	for i := 0; i < 40; i++ {

		waitgroup.Add(1)
		go func(index int32) {

			defer waitgroup.Done()
			l.Lock()
			defer l.Unlock()

			obj, err := p.Get()
			if err != nil {
				t.Fatalf("Expected no error, got %s", err)
			}
			casted := obj.Resource().(*resource_symulator)
			check[casted.id] = true

		}(int32(i))
	}

	waitgroup.Wait()

	for i := 1; i <= 40; i++ {
		if check[int32(i)] == false {
			t.Fatalf("Resource %d unused", i)
		}
	}
}

func TestPoolClose(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50

	create := func() (interface{}, error) {
		return resourceNew()
	}
	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}
	test := func(r interface{}) error {
		return nil
	}

	p := NewPool(min, max, create, destroy, test, nil)
	p.Close()
	_, err := p.Get()
	if err != PoolClosedError {
		t.Fatal("Expected Pool Closed Error got", err)
	}
}

func TestAddingABumResource(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	i := 0

	create := func() (interface{}, error) {

		i++
		if i%2 == 0 {
			return nil, errors.New("Create Error")
		}

		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p := NewPool(min, max, create, destroy, test, nil)
	defer p.Close()

	wg := sync.WaitGroup{}

	for index := 0; index < 50; index++ {
		r, err := p.Get()
		if err != nil {
			t.Fatal("Expected no error")
		}
		wg.Add(1)
		go func(r ResourcePoolWrapper) {
			defer wg.Done()
			r.Close()
		}(r)
	}

	wg.Wait()

	stats := p.Stats()
	if stats.InUse != 0 {
		t.Fatal("Expected 0 in use")
	}
}

func TestCreateError(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i int = 0

	create := func() (interface{}, error) {
		i++
		return nil, errors.New("Some error")
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p := NewPool(min, max, create, destroy, test, nil)
	defer p.Close()

	r, _ := p.Get()
	r, _ = p.Get()

	if r != nil {
		t.Fatal("Exepect r to be nil")
	}

	stats := p.Stats()
	if stats.InUse != 0 {
		t.Fatal("Expected 0 in use")
	}

	//throw away
	p.Get()
	p.Get()

	stats = p.Stats()
	if stats.InUse != 0 {
		t.Fail()
	}

	if stats.AvailableNow != 0 {
		t.Fail()
	}

	if stats.ResourcesOpen != 0 {
		t.Fail()
	}

}

func TestTest(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i uint32 = 0
	var tested uint32 = 0

	create := func() (interface{}, error) {
		i++
		return resourceNew()
	}

	destroy := func(r interface{}) {

	}

	test := func(r interface{}) error {
		tested++
		return errors.New("Reuse Error")
	}

	p := NewPool(min, max, create, destroy, test, nil)
	defer p.Close()

	//get a resource
	r, e := p.Get()
	r.Close()

	//get another with a failing test
	r, e = p.Get()
	if e != nil {
		t.Fatal("expected no error")
	}
}

const (
	bmin  = 5
	bmax  = 50
	bgets = 100
)

func BenchmarkPool(b *testing.B) {

	create := func() (interface{}, error) {
		r := new(resource_symulator)
		//assum that some real amount of work is being done here
		time.Sleep(time.Millisecond)
		return r, nil
	}

	destroy := func(r interface{}) {
		_ = r.(*resource_symulator)
	}

	test := func(r interface{}) error {
		return nil
	}

	p := NewPool(bmin, bmax, create, destroy, test, nil)

	for i := 0; i < b.N; i++ {

		for v := 0; v < bgets; v++ {
			r, _ := p.Get()
			r.Close()
		}
	}

}

func benchmarkRealWork(b *testing.B, n int) {

	create := func() (interface{}, error) {
		r := new(resource_symulator)
		//assum that some real amount of work is being done here
		time.Sleep(time.Millisecond)
		return r, nil
	}

	destroy := func(r interface{}) {
		_ = r.(*resource_symulator)
	}

	test := func(r interface{}) error {
		return nil
	}

	for nt := 0; nt < b.N; nt++ {
		p := NewPool(bmin, bmax, create, destroy, test, nil)

		//10 people all getting stuff waiting a milisecond and returning the connection
		wg := sync.WaitGroup{}
		for i := 0; i < bmin; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, _ := p.Get()
				time.Sleep(time.Millisecond)
				r.Close()
			}()
		}

		wg.Wait()
	}

}

func BenchmarkRealWorkMin(b *testing.B) {
	benchmarkRealWork(b, bmin)
}

func BenchmarkRealWork2Min(b *testing.B) {
	benchmarkRealWork(b, bmin*2)
}

func BenchmarkRealWorkMax(b *testing.B) {
	benchmarkRealWork(b, bmax)
}

func BenchmarkRealWork2Max(b *testing.B) {
	benchmarkRealWork(b, bmax*2)
}
