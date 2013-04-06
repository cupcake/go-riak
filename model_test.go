package riak

import (
	"github.com/bmizerany/assert"
	"testing"
	"time"
)

type DocumentModel struct {
	FieldS string  `riak:"string_field"`
	FieldF float64 `riak:"float_field"`
	FieldB bool
	Model
}

func TestModel(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)
	// Check that the JSON is correct
	t.Logf(string(doc.robject.Data))
	assert.T(t, `{"string_field":"text","float_field":1.2,"FieldB":true}` == string(doc.robject.Data))

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DocumentModel{}
	err = client.Load("testmodel.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	assert.T(t, doc2.FieldF == doc.FieldF)
	assert.T(t, doc2.FieldB == doc.FieldB)

	// Cleanup
	err = doc2.Delete()
	assert.T(t, err == nil)

	// Get the key
	key, err := client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "TestModelKey")
	// Set it differently
	err = client.SetKey("newTestModelKey", &doc2)
	assert.T(t, err == nil)
	// And test that it changed by getting it again
	key, err = client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "newTestModelKey")

	// Test Delete(), so test if the cleanup worked
	doc3 := DocumentModel{}
	err = client.Load("testmodel.go", "TestModelKey", &doc3)
	assert.T(t, err == NotFound)
}

type DocumentModelWithLinks struct {
	FieldS string
	ALink  One `riak:"tag_as_parent"`
	BLink  One // Will automatically use own name as a tag when linking
	Model
}

func TestModelWithLinks(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" to use as a parent and save it
	parent := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &parent)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = parent.Save()
	assert.T(t, err == nil)

	// Create a new DocumentModelWithLinks and save it, adding a link to the parent
	doc := DocumentModelWithLinks{FieldS: "textinlinked", ALink: One{model: &parent}}
	err = doc.BLink.Set(&parent) // testing One.Set while we're at it
	assert.T(t, err == nil)
	err = client.New("testmodellinks.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the struct are set correctly, including the link to the parent
	doc2 := DocumentModelWithLinks{}
	err = client.Load("testmodellinks.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	assert.T(t, doc2.ALink.model == nil) // Related documents are not loaded automatically, only the link is populated
	assert.T(t, doc2.ALink.link.Tag == "tag_as_parent")
	assert.T(t, doc2.BLink.link.Tag == "BLink")
	t.Logf("Testing DocumentModelWithLinks - One - %v - %v\n", doc2.ALink.model, doc2.ALink.link)
	t.Logf("Testing DocumentModelWithLinks - One - %v - %v\n", doc2.BLink.model, doc2.BLink.link)

	// Load the parent from the link
	parent2 := DocumentModel{}
	err = doc2.ALink.Get(&parent2)
	assert.T(t, err == nil)
	assert.T(t, parent.FieldS == parent2.FieldS)
	assert.T(t, parent.FieldF == parent2.FieldF)
	assert.T(t, parent.FieldB == parent2.FieldB)
	assert.T(t, parent.Key() == parent2.Key())

	// Cleanup
	bucket, _ := client.Bucket("testmodel.go")
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
	bucket, _ = client.Bucket("testmodellinks.go")
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

type FriendLinks struct {
	Friends Many "friend"
	Model
}

func TestModelWithManyLinks(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create two new "DocumentModel"s to use as friends and save it
	f1 := DocumentModel{FieldS: "friend1", FieldF: 1.0, FieldB: true}
	err := client.New("testmodel.go", "f1", &f1)
	assert.T(t, err == nil)
	err = f1.Save()
	assert.T(t, err == nil)
	f2 := DocumentModel{FieldS: "friend2", FieldF: 2.0, FieldB: true}
	err = client.New("testmodel.go", "f2", &f2)
	assert.T(t, err == nil)
	err = f2.Save()
	assert.T(t, err == nil)

	// Create a new "FriendLinks" to and save it
	doc := FriendLinks{Friends: Many{One{model: &f1}}}
	// Testing Many.Add while we're at it.
	err = doc.Friends.Add(&f2)
	assert.T(t, err == nil)
	err = client.New("testmodel.go", "TestMany", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	t.Logf("Friends json - %v\n", string(doc.robject.Data))

	// Now load a new document and verify it has two links
	var doc2 FriendLinks
	err = client.Load("testmodel.go", "TestMany", &doc2)
	assert.T(t, err == nil)
	assert.T(t, len(doc2.Friends) == 2)
	for i, v := range doc2.Friends {
		var f DocumentModel
		err = v.Get(&f)
		assert.T(t, err == nil)
		t.Logf("TestingModelWithManyLinks - %v - %v - %v\n", i, v, f)
	}
}

type ComplexModel struct {
	Flags map[string]bool `riak:"a"`
	Model
}

func (c *ComplexModel) Resolve(count int) (err error) {
	siblings := make([]ComplexModel, count)
	err = c.GetSiblings(siblings)
	if err != nil {
		return err
	}
	for _, s := range siblings {
		for key, flag := range s.Flags {
			if flag {
				c.Flags[key] = true
			}
		}
	}
	return
}

func TestConflictingModel(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a bucket where siblings are allowed
	bucket, err := client.Bucket("testconflict.go")
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	// Delete earlier work ...
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)

	// Create a new "CompleModel" and save it
	m1 := ComplexModel{Flags: map[string]bool{"a": true, "b": false}}
	err = client.New("testconflict.go", "TestModelKey", &m1)
	assert.T(t, err == nil)
	err = m1.Save()
	assert.T(t, err == nil)

	// Create the same again (with the same key)
	m2 := ComplexModel{Flags: map[string]bool{"a": false, "b": true, "c": true}}
	err = client.New("testconflict.go", "TestModelKey", &m2)
	assert.T(t, err == nil)
	err = m2.Save()
	assert.T(t, err == nil)

	// Now load it from Riak to test conflicts
	m3 := ComplexModel{}
	err = client.Load("testconflict.go", "TestModelKey", &m3)
	t.Logf("Loading model - %v\n", err)
	t.Logf("ComplexModel = %v\n", m3)
	assert.T(t, err == nil)
	assert.T(t, m3.Flags["a"])
	assert.T(t, m3.Flags["b"])
	assert.T(t, m3.Flags["c"])

	// Cleanup
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

type DMTime struct {
	FieldS string
	FieldT time.Time
	Model
}

func TestModelTime(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create and save
	doc := DMTime{FieldS: "text", FieldT: time.Now()}
	err := client.New("testmodel.go", "TestTime", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DMTime{}
	err = client.Load("testmodel.go", "TestTime", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	t.Logf("FieldT= %v ? %v\n", doc2.FieldT, doc.FieldT)
	assert.T(t, doc2.FieldT.Equal(doc.FieldT))
}

type SubStruct struct {
	Value string `riak:"value"`
}

type DMInclude struct {
	Name string    `riak:"name"`
	Sub  SubStruct `riak:"sub"`
	Model
}

func TestModelIncludingOtherStruct(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create and save
	doc := DMInclude{Name: "some name", Sub: SubStruct{Value: "some value"}}
	err := client.New("testmodel.go", "TestModelIncludingOtherStruct", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DMInclude{}
	err = client.Load("testmodel.go", "TestModelIncludingOtherStruct", &doc2)
	t.Logf("doc2 json = %v\n", string(doc2.robject.Data))
	assert.T(t, err == nil)
	assert.T(t, string(doc2.robject.Data) == `{"name":"some name","sub":{"value":"some value"}}`)
	assert.T(t, doc2.Name == doc.Name)
	t.Logf("Sub struct = %v ? %v\n", doc2.Sub.Value, doc.Sub.Value)
	assert.T(t, doc2.Sub.Value == doc.Sub.Value)
}

func TestModelReload(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)

	doc2 := DocumentModel{FieldS: "text22", FieldF: 1.4, FieldB: true}
	err = client.New("testmodel.go", "TestModelKey", &doc2)
	err = doc2.Save()
	assert.T(t, err == nil)

	vclock := string(doc.robject.Vclock)
	err = (&doc).Reload()
	assert.T(t, err == nil)
	assert.T(t, string(doc.robject.Vclock) != vclock)
	assert.T(t, string(doc.robject.Vclock) == string(doc2.robject.Vclock))
	assert.T(t, doc.FieldS == doc2.FieldS)
	assert.T(t, doc.FieldF == doc2.FieldF)
	assert.T(t, doc.FieldB == doc2.FieldB)
}
