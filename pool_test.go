package pool

import (
	"testing"
	"time"
)

var (
	no int
)

type resource_symulator struct {
	id int
}

func resourceNew() (r *resource_symulator, err error) {
	no++
	r = new(resource_symulator)
	r.id = no
	time.Sleep(time.Microsecond * 1)
	return
}

func (r *resource_symulator) resourceDel() (err error) {
	r.id = 0
	time.Sleep(time.Microsecond * 1)
	return
}

func TestIntialize(t *testing.T) {
	var db *resource_symulator
	var err error

	create := func() (interface{}, error) {
		db, err = resourceNew()
		return db, err
	}

	destroy := func(r interface{}) error {
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(2, 5, create, destroy, test)
	if err != nil {
		t.Fatalf("Resource error: %s", err.Error())
	}
	msg, err := p.Get()
	if err != nil {
		t.Fatalf("Get Resource error: %s", err.Error())
	}
	if msg.Resource.(*resource_symulator).id != 1 {
		t.Fatalf("Resource id should be on = %d", msg)
	}
}

func TestBeyond(t *testing.T) {

	var db *resource_symulator
	var err error
	create := func() (interface{}, error) {
		db, err = resourceNew()
		return db, err
	}
	destroy := func(r interface{}) error {
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(2, 5, create, destroy, test)
	if err != nil {
		t.Fatalf("Resource error: %s", err.Error())
	}

	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()

	if err == nil {
		t.Fatalf("Must error on sixth get")
	}

	if _, isResourceExhausted := err.(ResourceExhaustedError); isResourceExhausted == false {
		t.Fatalf("Error must be ResourceExhaustedError")
	}
}

func TestWait(t *testing.T) {

	var db *resource_symulator
	var err error
	create := func() (interface{}, error) {
		db, err = resourceNew()
		return db, err
	}

	destroy := func(r interface{}) error {
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(2, 5, create, destroy, test)
	if err != nil {
		t.Fatalf("Resource error: %s", err.Error())
	}

	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	msg, err := p.Get()

	go func() {
		msg.Close()
	}();

	msg, err = p.Get()

}


func TestResourceRelease(t *testing.T) {
	var db *resource_symulator
	var err error
	create := func() (interface{}, error) {
		db, err = resourceNew()
		return db, err
	}
	destroy := func(r interface{}) error {
		return db.resourceDel()
	}
	test := func(r interface{}) error {
		return nil
	}

	var min, max uint32
	min = 10
	max = 50
	p, err := NewPool(min, max, create, destroy, test)

	if p.Cap() != max {
		t.Fatalf("Cap size incorrect. Should be %d but is %d", max, cap(p.resources))
	}

	msg, err := p.Get()
	if err != nil {
		t.Fatalf("get error %d", err)
	}

	if p.AvailableNow() != min - 1 {
		t.Fatalf("AvailableNow size incorrect. Should be %d but is %d", min - 1, len(p.resources))
	}

	p.Release(msg)
	if min != p.AvailableNow() {
		t.Fatalf("AvailableNow size incorrect. Should be %d but is %d", min, p.AvailableNow())
	}

	var dbuse = make(map[uint32]ResourceWrapper)
	for i := uint32(0); i < max; i++ {
		dbuse[i], err = p.Get()
		if err != nil {
			t.Fatalf("get error %d", err)
		}
	}

	for _, v := range dbuse {
		p.Destroy(v)
	}

	if p.Cap() != max {
		t.Fatalf("Pool cap incorrect. Should be %d but is %d", max, p.Cap())
	}

	// pools test
	po := uint32(50)
	for i := uint32(0); i < po; i++ {
		dbuse[i], err = p.Get()
		if err != nil {
			t.Fatalf("get error %d", err)
		}
	}

	if p.InUse() != po {
		t.Fatalf("Pool InUse() before release incorrect. Should be 0 but is %d", p.InUse())
	}
	if p.AvailableMax() != p.Cap()-po {
		t.Fatalf("Pool AvailableMax() incorrect. Should be  %d but is %d", max-po, p.AvailableMax())
	}
	for i := uint32(0); i < po; i++ {
		p.Release(dbuse[i])
	}
	if p.InUse() != 0 {
		t.Fatalf("Pool InUse() incorrect. Should be 0 but is %d", p.InUse())
	}
	if p.Cap() != max {
		t.Fatalf("Pool Cap() incorrect. Should be %d but is %d", max, p.Cap())
	}
	if p.AvailableNow() < min || p.AvailableNow() > max {
		t.Fatalf("Pool AvailableNow() incorrect. Should be min %d, max %d but is %d", min, max, p.AvailableNow())
	}
	if p.AvailableMax() != p.Cap() {
		t.Fatalf("Pool AvailableMax() incorrect. Should be  %d but is %d", max, p.AvailableMax())
	}
}

func TestClose(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i int
	var db *resource_symulator
	var err error
	create := func() (interface{}, error) {
		db, err = resourceNew()
		return db, err
	}
	destroy := func(r interface{}) error {
		i++
		return db.resourceDel()
	}
	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(min, max, create, destroy, test)
	count := int(p.Count())
	p.Close()
	if i != count {
		t.Errorf("Close was not called correct times. It was called %d and should have been called  %d times", i, count)
	}
}
