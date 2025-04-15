package postgres

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"runtime"
	"strconv"

	"github.com/wardaBibi/mongo-postgres-benchmark/idgen"
	"github.com/wardaBibi/mongo-postgres-benchmark/record"
	"github.com/jackc/pgx/v4/pgxpool" 
	"golang.org/x/net/context"
)

// FuncProvider implements dbProvider for PostgreSQL using pgxpool.
type FuncProvider struct {
	DB        *pgxpool.Pool // Change from pgx.Conn to pgxpool.Pool
	TableName string
}

// InsertRecord generates a new random record and inserts it with an ID provided
// by id.GetNew as a JSON-encoded string.
func (p *FuncProvider) InsertRecord(data *record.Person, id idgen.Generator, rnd *rand.Rand) bool {
	data.Randomise(rnd)
	data.ID = id.GetNew()

	jsonData, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	_, err = p.DB.Exec(context.Background(), "INSERT INTO "+p.TableName+" (data) VALUES ($1)", string(jsonData))
	if err != nil {
		log.Println(err)
		return false
	}

	return true
}

// UpdateRecord attempts to update the record with ID returned by
// id.GetExisting.
func (p *FuncProvider) UpdateRecord(_ *record.Person, id idgen.Generator, rnd *rand.Rand) bool {
	recordID := id.GetExisting()
	_, err := p.DB.Exec(
		context.Background(),
		"UPDATE "+p.TableName+" SET data=jsonb_set(data, '{balance}', $1::jsonb, false) WHERE data->'id'=$2",
		strconv.FormatFloat(float64(rnd.Float32()), 'f', -1, 32),
		recordID,
	)
	if err != nil {
		log.Println(recordID, err)
		return false
	}
	return true
}

// ReadRecord attempts to fetch the record with an ID returned by
// id.GetExisting.
func (p *FuncProvider) ReadRecord(_ *record.Person, id idgen.Generator, _ *rand.Rand) bool {
	recordID := id.GetExisting()

	var rawData []byte
	err := p.DB.QueryRow(context.Background(), "SELECT data FROM "+p.TableName+" WHERE data->'id'=$1", recordID).Scan(&rawData)
	if err != nil {
		log.Println(recordID, err)
		return false
	}

	var data = &record.Person{}
	if err := json.Unmarshal(rawData, &data); err != nil {
		log.Println(recordID, err)
		return false
	}

	return true
}

// ReadRange performs a range query on the age field.
func (p *FuncProvider) ReadRange(_ *record.Person, _ idgen.Generator, _ *rand.Rand) bool {
	rows, err := p.DB.Query(context.Background(), "SELECT data FROM "+p.TableName+" WHERE (data->'age') > '45' AND (data->'age') < '75'")
	if err != nil {
		log.Println(err)
		return false
	}
	defer rows.Close()

	var rawData []byte
	var data = &record.Person{}
	for rows.Next() {
		if err := rows.Scan(&rawData); err != nil {
			log.Println(err)
			return false
		}

		if err := json.Unmarshal(rawData, &data); err != nil {
			log.Println(err)
			return false
		}
	}

	return true
}

// ReadMostRecentRecord fetches the most recently inserted record by performing
// a sort on the ID field, and limiting the results to a single record.
func (p *FuncProvider) ReadMostRecentRecord(_ *record.Person, _ idgen.Generator, _ *rand.Rand) bool {
	var rawData []byte
	err := p.DB.QueryRow(context.Background(), "SELECT data FROM "+p.TableName+" ORDER BY data->'id' DESC LIMIT 1").Scan(&rawData)
	if err != nil {
		log.Println(err)
		return false
	}

	var data = &record.Person{}
	if err := json.Unmarshal(rawData, &data); err != nil {
		log.Println(err)
		return false
	}

	return true
}

// GetMaxID returns the highest ID in the table.
func (p *FuncProvider) GetMaxID() (uint64, error) {
	var count uint64
	err := p.DB.QueryRow(context.Background(), "SELECT data->'id' FROM "+p.TableName+" ORDER BY data->'id' DESC LIMIT 1").Scan(&count)
	if err != nil || count == 0 {
		return 0, fmt.Errorf("no existing data? error = %v, count = %d", err, count)
	}

	return count, nil
}

// NewProvider returns an instance of FuncProvider.
func NewProvider(endpoint *url.URL, tableName string) (*FuncProvider, error) {
	// Connect to PostgreSQL using pgxpool
	config, err := pgxpool.ParseConfig(endpoint.String())
	if err != nil {
		return nil, err
	}

	// Open a pool of connections
	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	// Ensure the connection pool is alive
	if err := pool.Ping(context.Background()); err != nil {
		return nil, err
	}

	// BUG: work around Go garbage collection bug:
	//
	// https://github.com/golang/go/issues/21056
	runtime.GOMAXPROCS(2)

	// DB func provider
	return &FuncProvider{
		DB:        pool,
		TableName: tableName,
	}, nil
}
