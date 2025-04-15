package mongo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wardaBibi/mongo-postgres-benchmark/idgen"
	"github.com/wardaBibi/mongo-postgres-benchmark/record"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// FuncProvider implements dbProvider for MongoDB using the MongoDB Go Driver
type FuncProvider struct {
	Client     *mongo.Client
	Collection *mongo.Collection
}

// InsertRecord generates a new random record and inserts it with an ID provided
// by id.GetNew
func (p *FuncProvider) InsertRecord(data *record.Person, id idgen.Generator, rnd *rand.Rand) bool {
	// Get a new context with timeout (for cancellation, if necessary)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data.Randomise(rnd)
	data.ID = id.GetNew()

	_, err := p.Collection.InsertOne(ctx, data)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

// UpdateRecord attempts to update the record with ID returned by id.GetExisting.
func (p *FuncProvider) UpdateRecord(_ *record.Person, id idgen.Generator, rnd *rand.Rand) bool {
	// Get a new context with timeout (for cancellation, if necessary)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recordID := id.GetExisting()

	update := bson.M{
		"$set": bson.M{"balance": rnd.Float32()},
	}
	_, err := p.Collection.UpdateOne(ctx, bson.M{"_id": recordID}, update)
	if err != nil {
		log.Println(recordID, err)
		return false
	}

	return true
}

// ReadRecord attempts to fetch the record with an ID returned by id.GetExisting.
func (p *FuncProvider) ReadRecord(_ *record.Person, id idgen.Generator, rnd *rand.Rand) bool {
	// Get a new context with timeout (for cancellation, if necessary)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recordID := id.GetExisting()
	var data record.Person

	err := p.Collection.FindOne(ctx, bson.M{"_id": recordID}).Decode(&data)
	if err != nil {
		log.Println(recordID, err)
		return false
	}

	return true
}

// ReadRange performs a range query on the age field.
func (p *FuncProvider) ReadRange(_ *record.Person, _ idgen.Generator, _ *rand.Rand) bool {
	// Get a new context with timeout (for cancellation, if necessary)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{
		"age": bson.M{
			"$gt": 45,
			"$lt": 75,
		},
	}

	cursor, err := p.Collection.Find(ctx, filter)
	if err != nil {
		log.Println(err)
		return false
	}
	defer cursor.Close(ctx)

	// Read all documents in the result set
	for cursor.Next(ctx) {
		var data record.Person
		if err := cursor.Decode(&data); err != nil {
			log.Println(err)
			return false
		}
	}

	if err := cursor.Err(); err != nil {
		log.Println(err)
		return false
	}

	return true
}

// ReadMostRecentRecord fetches the most recently inserted record.
func (p *FuncProvider) ReadMostRecentRecord(_ *record.Person, _ idgen.Generator, _ *rand.Rand) bool {
	// Get a new context with timeout (for cancellation, if necessary)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})
	var data record.Person

	err := p.Collection.FindOne(ctx, bson.M{}, opts).Decode(&data)
	if err != nil {
		log.Println(err)
		return false
	}

	return true
}

// GetMaxID returns the largest ID in the collection.
func (p *FuncProvider) GetMaxID() (uint64, error) {
	// Get a new context with timeout (for cancellation, if necessary)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})
	var result struct {
		ID uint64 `bson:"_id"`
	}

	err := p.Collection.FindOne(ctx, bson.M{}, opts).Decode(&result)
	if err != nil {
		return 0, fmt.Errorf("no existing data? %v", err)
	}

	return result.ID, nil
}

func NewProvider(endpoint *url.URL, tableName string) (*FuncProvider, error) {
	dbName := strings.TrimPrefix(endpoint.Path, "/")
	if dbName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	q := endpoint.Query()

	// --- ReadConcern ---
	var rc *readconcern.ReadConcern
	switch strings.ToLower(q.Get("readConcern")) {
	case "", "majority":
		rc = readconcern.Majority()
	case "local":
		rc = readconcern.Local()
	case "linearizable":
		rc = readconcern.Linearizable()
	default:
		return nil, errors.New("unknown readConcern value")
	}
	q.Del("readConcern")

	// --- WriteConcern ---
	var wc *writeconcern.WriteConcern
	switch w := strings.ToLower(q.Get("writeConcern")); w {
	case "", "majority":
		wc = writeconcern.New(writeconcern.WMajority())
	default:
		n, err := strconv.Atoi(w)
		if err != nil {
			return nil, fmt.Errorf("invalid writeConcern value: %v", err)
		}
		wc = writeconcern.New(writeconcern.W(n))
	}
	q.Del("writeConcern")

	// --- Journal ---
	switch strings.ToLower(q.Get("journal")) {
	case "", "false", "0":
		// do nothing (journal = false)
	case "true", "1":
		wc = wc.WithOptions(writeconcern.J(true))
	default:
		return nil, errors.New("unknown journal value")
	}
	q.Del("journal")

	// --- FSync ---
	switch strings.ToLower(q.Get("fsync")) {
	case "", "false", "0":
		// No-op
	case "true", "1":
		log.Println("Note: fsync=true requested, but ignored (unsupported in mongo-go-driver)")
	default:
		return nil, errors.New("unknown fsync value")
	}
	q.Del("fsync")

	// --- Clean up the URL for MongoDB ---
	endpoint.RawQuery = q.Encode()

	clientOpts := options.Client().
		ApplyURI(endpoint.String()).
		SetWriteConcern(wc).
		SetReadConcern(rc)

	client, err := mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(context.Background(), nil); err != nil {
		return nil, err
	}

	collection := client.Database(dbName).Collection(tableName)

	return &FuncProvider{
		Client:     client,
		Collection: collection,
	}, nil
}


