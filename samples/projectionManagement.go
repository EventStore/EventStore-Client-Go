package samples

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/gofrs/uuid"
)

func CreateClient(connectionString string) {
	// region createClient
	conf, err := esdb.ParseConnectionString(connectionString)

	if err != nil {
		panic(err)
	}

	client, err := esdb.NewProjectionClient(conf)

	if err != nil {
		panic(err)
	}
	// endregion createClient

	defer client.Close()
}

func Disable(client *esdb.ProjectionClient) {
	// region disable
	err := client.Disable(context.Background(), "$by_category", esdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion disable
}

func DisableNotFound(client *esdb.ProjectionClient) {
	// region disableNotFound
	err := client.Disable(context.Background(), "projection that doesn't exist", esdb.GenericProjectionOptions{})

	if esdbError, ok := esdb.FromError(err); !ok {
		if esdbError.IsErrorCode(esdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion disableNotFound
}

func Enable(client *esdb.ProjectionClient) {
	// region enable
	err := client.Enable(context.Background(), "$by_category", esdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion enable
}

func EnableNotFound(client *esdb.ProjectionClient) {
	// region enableNotFound
	err := client.Enable(context.Background(), "projection that doesn't exist", esdb.GenericProjectionOptions{})

	if esdbError, ok := esdb.FromError(err); !ok {
		if esdbError.IsErrorCode(esdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion enableNotFound
}

func Delete(client *esdb.ProjectionClient) {
	// region delete
	err := client.Delete(context.Background(), "$by_category", esdb.DeleteProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion delete
}

func DeleteNotFound(client *esdb.ProjectionClient) {
	// region deleteNotFound
	err := client.Delete(context.Background(), "projection that doesn't exist", esdb.DeleteProjectionOptions{})

	if esdbError, ok := esdb.FromError(err); !ok {
		if esdbError.IsErrorCode(esdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion deleteNotFound
}

func Abort(client *esdb.ProjectionClient) {
	// region abort
	err := client.Abort(context.Background(), "$by_category", esdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion abort
}

func AbortNotFound(client *esdb.ProjectionClient) {
	// region abortNotFound
	err := client.Abort(context.Background(), "projection that doesn't exist", esdb.GenericProjectionOptions{})

	if esdbError, ok := esdb.FromError(err); !ok {
		if esdbError.IsErrorCode(esdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion abortNotFound
}

func Reset(client *esdb.ProjectionClient) {
	// region reset
	err := client.Reset(context.Background(), "$by_category", esdb.ResetProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion reset
}

func ResetNotFound(client *esdb.ProjectionClient) {
	// region resetNotFound
	err := client.Reset(context.Background(), "projection that doesn't exist", esdb.ResetProjectionOptions{})

	if esdbError, ok := esdb.FromError(err); !ok {
		if esdbError.IsErrorCode(esdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion resetNotFound
}

func Create(client *esdb.ProjectionClient) {
	// region createContinuous
	script := `
fromAll()
.when({
	$init:function(){
		return {
			count: 0
		}
	},
	myEventUpdatedType: function(state, event){
		state.count += 1;
	}
})
.transformBy(function(state){
	state.count = 10;
})
.outputState()
`
	name := fmt.Sprintf("countEvent_Create_%s", uuid.Must(uuid.NewV4()))
	err := client.Create(context.Background(), name, script, esdb.CreateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	// endregion createContinuous
}

func CreateConflict(client *esdb.ProjectionClient) {
	script := ""
	name := ""

	// region createContinuousConflict
	err := client.Create(context.Background(), name, script, esdb.CreateProjectionOptions{})

	if esdbErr, ok := esdb.FromError(err); !ok {
		if esdbErr.IsErrorCode(esdb.ErrorCodeUnknown) && strings.Contains(esdbErr.Err().Error(), "Conflict") {
			log.Printf("projection %s already exists", name)
			return
		}
	}
	// endregion createContinuousConflict
}

func Update(client *esdb.ProjectionClient) {
	script := ""
	newScript := ""
	name := ""

	// region update
	err := client.Create(context.Background(), name, script, esdb.CreateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	err = client.Update(context.Background(), name, newScript, esdb.UpdateProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion update

}

func UpdateNotFound(client *esdb.ProjectionClient) {
	script := ""

	// region updateNotFound
	err := client.Update(context.Background(), "projection that doesn't exist", script, esdb.UpdateProjectionOptions{})

	if esdbError, ok := esdb.FromError(err); !ok {
		if esdbError.IsErrorCode(esdb.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion updateNotFound
}

func ListAll(client *esdb.ProjectionClient) {
	// region listAll
	projections, err := client.ListAll(context.Background(), esdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}

	for i := range projections {
		projection := projections[i]

		log.Printf(
			"%s, %s, %s, %s, %f",
			projection.Name,
			projection.Status,
			projection.CheckpointStatus,
			projection.Mode,
			projection.Progress,
		)
	}
	// endregion listAll
}

func List(client *esdb.ProjectionClient) {
	// region listContinuous
	projections, err := client.ListContinuous(context.Background(), esdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}

	for i := range projections {
		projection := projections[i]

		log.Printf(
			"%s, %s, %s, %s, %f",
			projection.Name,
			projection.Status,
			projection.CheckpointStatus,
			projection.Mode,
			projection.Progress,
		)
	}
	// endregion listContinuous
}

func GetStatus(client *esdb.ProjectionClient) {
	// region getStatus
	projection, err := client.GetStatus(context.Background(), "$by_category", esdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}

	log.Printf(
		"%s, %s, %s, %s, %f",
		projection.Name,
		projection.Status,
		projection.CheckpointStatus,
		projection.Mode,
		projection.Progress,
	)
	// endregion getStatus
}

func GetState(client *esdb.ProjectionClient) {
	projectionName := ""
	// region getState
	type Foobar struct {
		Count int64
	}

	value, err := client.GetState(context.Background(), projectionName, esdb.GetStateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	jsonContent, err := value.MarshalJSON()

	if err != nil {
		panic(err)
	}

	var foobar Foobar

	if err = json.Unmarshal(jsonContent, &foobar); err != nil {
		panic(err)
	}

	log.Printf("count %d", foobar.Count)
	// endregion getState
}

func GetResult(client *esdb.ProjectionClient) {
	projectionName := ""
	// region getResult
	type Baz struct {
		Result int64
	}

	value, err := client.GetResult(context.Background(), projectionName, esdb.GetResultProjectionOptions{})

	if err != nil {
		panic(err)
	}

	jsonContent, err := value.MarshalJSON()

	if err != nil {
		panic(err)
	}

	var baz Baz

	if err = json.Unmarshal(jsonContent, &baz); err != nil {
		panic(err)
	}

	log.Printf("result %d", baz.Result)
	// endregion getResult
}

func RestartSubSystem(client *esdb.ProjectionClient) {
	// region restartSubsystem
	err := client.RestartSubsystem(context.Background(), esdb.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion restartSubsystem
}
