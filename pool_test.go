package pool

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

	p, _ := NewPool(2, 5, create, destroy, test, nil)
	defer p.Close()

	msg, err := p.Get()
	if err != nil {
		t.Fatalf("Get Resource error: %s", err.Error())
	}

	if msg.Resource.(*resource_symulator).id != 1 {
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

	p, f := NewPool(2, 5, create, destroy, test, nil)
	if <-f != nil {
		t.Fatal("Expected no error")
	}

	defer p.Close()
	const d = time.Millisecond * 50

	if _, err = p.getAvailable(time.After(d)); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(time.After(d)); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(time.After(d)); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(time.After(d)); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(time.After(d)); err != nil {
		t.Fatal(err)
	}

	if _, err = p.getAvailable(time.After(d)); err == nil {
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

	p, _ := NewPool(2, 5, create, destroy, test, nil)
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
	p, _ := NewPool(min, max, create, destroy, test, nil)
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
			casted := obj.Resource.(*resource_symulator)
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

func TestResourceRelease(t *testing.T) {

	var err error
	var destroys uint32 = 0

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {

		destroys++
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	var min, max uint32
	min = 10
	max = 50
	p, fillChan := NewPool(min, max, create, destroy, test, nil)
	defer p.Close()
	<-fillChan //wait for the pool to fill

	msg, err := p.Get()
	if err != nil {
		t.Fatalf("get error %d", err)
	}

	if p.AvailableNow() != min-1 {
		t.Fatalf("AvailableNow size incorrect. Should be %d but is %d", min-1, len(p.resources))
	}

	msg.Close()
	if min != p.AvailableNow() {
		t.Fatalf("AvailableNow size incorrect. Should be %d but is %d", min, p.AvailableNow())
	}

	var dbuse = make(map[uint32]ResourceWrapper)
	for i := uint32(0); i < max; i++ {
		dbuse[i], err = p.Get()
		if err != nil {
			t.Fatalf("get error %d, %d", i, err)
		}
	}

	for _, v := range dbuse {
		v.Destroy()
	}

	if destroys != max {
		t.Fatalf("Expected %d destroys got %d", max, destroys)
	}

	// pools test
	po := uint32(50)
	for i := uint32(0); i < po; i++ {
		dbuse[i], err = p.Get()
		if err != nil {
			t.Fatalf("get error %d", err)
		}
	}

	stat := p.Stats()

	if p.InUse() != po {
		t.Fatalf("Pool InUse() before release incorrect. Should be 0 but is %d", p.InUse())
	}

	if stat.InUse != po {
		t.Fatalf("Pool InUse() before release incorrect. Should be 0 but is %d", p.InUse())
	}

	for i := uint32(0); i < po; i++ {
		value := dbuse[i]
		value.Close()
	}

	if p.InUse() != 0 {
		t.Fatalf("Pool InUse() incorrect. Should be 0 but is %d", p.InUse())
	}

	if p.AvailableNow() < min || p.AvailableNow() > max {
		t.Fatalf("Pool AvailableNow() incorrect. Should be min %d, max %d but is %d", min, max, p.AvailableNow())
	}

}

func TestClose(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i int

	create := func() (interface{}, error) {
		return resourceNew()
	}
	destroy := func(r interface{}) {
		i++
		db := r.(*resource_symulator)
		db.resourceDel()
	}
	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(min, max, create, destroy, test, nil)

	count := int(p.ResourcesOpen())
	p.Close()
	if i != count {
		t.Errorf("Close was not called correct times. It was called %d and should have been called  %d times", i, count)
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

	p, _ := NewPool(min, max, create, destroy, test, nil)
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

	p, f := NewPool(min, max, create, destroy, test, nil)
	<-f
	defer p.Close()

	for index := 0; index < 50; index++ {
		func() {
			r, err := p.Get()
			if err == nil {
				defer r.Close()
			}
		}()
	}

	if p.InUse() != 0 {
		t.Fatal("Expected 0 in use")
	}

	//let the pool fill
	time.Sleep(time.Millisecond)

	if p.AvailableNow() != min {
		t.Fatalf("Expected available now to be at min(%d) actually(%d)", min, p.AvailableNow())
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

	p, _ := NewPool(min, max, create, destroy, test, nil)
	p.TimeoutTime = time.Microsecond
	defer p.Close()

	r, _ := p.Get()
	r, _ = p.Get()

	//shouldn't do anything
	r.Close()
	r.Destroy()

	if p.InUse() != 0 {
		t.Fail()
	}

	//throw away
	p.Get()
	p.Get()

	stats := p.Stats()
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

	p, fillChannel := NewPool(min, max, create, destroy, test, nil)
	p.TimeoutTime = time.Microsecond
	defer p.Close()
	<-fillChannel

	if i != min {
		t.Fatalf("Exepected %d new rources to be made, got %d", min, i)
	}

	//bum close
	r, e := p.Get()
	r.Close()

	if e == nil {
		t.Fatal("expected error")
	}

	_, e = p.Get()
	if e == nil {
		t.Fatal("expected error")
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

	p, f := NewPool(bmin, bmax, create, destroy, test, nil)
	<-f

	for i := 0; i < b.N; i++ {

		for v := 0; v < bgets; v++ {
			r, _ := p.Get()
			r.Close()
		}
	}

}

func BenchmarkRealWorkMin(b *testing.B) {

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
		p, f := NewPool(bmin, bmax, create, destroy, test, nil)
		<-f

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

func BenchmarkRealWork2Min(b *testing.B) {

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
		p, f := NewPool(bmin, bmax, create, destroy, test, nil)
		<-f

		//10 people all getting stuff waiting a milisecond and returning the connection
		wg := sync.WaitGroup{}
		for i := 0; i < bmin*2; i++ {
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

func BenchmarkRealWorkMax(b *testing.B) {

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
		p, f := NewPool(bmin, bmax, create, destroy, test, nil)
		<-f

		//10 people all getting stuff waiting a milisecond and returning the connection
		wg := sync.WaitGroup{}
		for i := 0; i < bmax; i++ {
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

func BenchmarkRealWork2Max(b *testing.B) {

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
		p, f := NewPool(bmin, bmax, create, destroy, test, nil)
		<-f

		//10 people all getting stuff waiting a milisecond and returning the connection
		wg := sync.WaitGroup{}
		for i := 0; i < bmax*2; i++ {
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
