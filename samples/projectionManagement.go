package samples

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

func CreateClient(connectionString string) {
	// region createClient
	conf, err := kurrent.ParseConnectionString(connectionString)

	if err != nil {
		panic(err)
	}

	client, err := kurrent.NewProjectionClient(conf)

	if err != nil {
		panic(err)
	}
	// endregion createClient

	defer client.Close()
}

func Disable(client *kurrent.ProjectionClient) {
	// region disable
	err := client.Disable(context.Background(), "$by_category", kurrent.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion disable
}

func DisableNotFound(client *kurrent.ProjectionClient) {
	// region disableNotFound
	err := client.Disable(context.Background(), "projection that doesn't exist", kurrent.GenericProjectionOptions{})

	if esdbError, ok := kurrent.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrent.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion disableNotFound
}

func Enable(client *kurrent.ProjectionClient) {
	// region enable
	err := client.Enable(context.Background(), "$by_category", kurrent.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion enable
}

func EnableNotFound(client *kurrent.ProjectionClient) {
	// region enableNotFound
	err := client.Enable(context.Background(), "projection that doesn't exist", kurrent.GenericProjectionOptions{})

	if esdbError, ok := kurrent.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrent.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion enableNotFound
}

func Delete(client *kurrent.ProjectionClient) {
	// region delete
	err := client.Delete(context.Background(), "$by_category", kurrent.DeleteProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion delete
}

func DeleteNotFound(client *kurrent.ProjectionClient) {
	// region deleteNotFound
	err := client.Delete(context.Background(), "projection that doesn't exist", kurrent.DeleteProjectionOptions{})

	if esdbError, ok := kurrent.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrent.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion deleteNotFound
}

func Abort(client *kurrent.ProjectionClient) {
	// region abort
	err := client.Abort(context.Background(), "$by_category", kurrent.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion abort
}

func AbortNotFound(client *kurrent.ProjectionClient) {
	// region abortNotFound
	err := client.Abort(context.Background(), "projection that doesn't exist", kurrent.GenericProjectionOptions{})

	if esdbError, ok := kurrent.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrent.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion abortNotFound
}

func Reset(client *kurrent.ProjectionClient) {
	// region reset
	err := client.Reset(context.Background(), "$by_category", kurrent.ResetProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion reset
}

func ResetNotFound(client *kurrent.ProjectionClient) {
	// region resetNotFound
	err := client.Reset(context.Background(), "projection that doesn't exist", kurrent.ResetProjectionOptions{})

	if esdbError, ok := kurrent.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrent.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion resetNotFound
}

func Create(client *kurrent.ProjectionClient) {
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
	name := fmt.Sprintf("countEvent_Create_%s", uuid.New())
	err := client.Create(context.Background(), name, script, kurrent.CreateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	// endregion createContinuous
}

func CreateConflict(client *kurrent.ProjectionClient) {
	script := ""
	name := ""

	// region createContinuousConflict
	err := client.Create(context.Background(), name, script, kurrent.CreateProjectionOptions{})

	if esdbErr, ok := kurrent.FromError(err); !ok {
		if esdbErr.IsErrorCode(kurrent.ErrorCodeUnknown) && strings.Contains(esdbErr.Err().Error(), "Conflict") {
			log.Printf("projection %s already exists", name)
			return
		}
	}
	// endregion createContinuousConflict
}

func Update(client *kurrent.ProjectionClient) {
	script := ""
	newScript := ""
	name := ""

	// region update
	err := client.Create(context.Background(), name, script, kurrent.CreateProjectionOptions{})

	if err != nil {
		panic(err)
	}

	err = client.Update(context.Background(), name, newScript, kurrent.UpdateProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion update

}

func UpdateNotFound(client *kurrent.ProjectionClient) {
	script := ""

	// region updateNotFound
	err := client.Update(context.Background(), "projection that doesn't exist", script, kurrent.UpdateProjectionOptions{})

	if esdbError, ok := kurrent.FromError(err); !ok {
		if esdbError.IsErrorCode(kurrent.ErrorCodeResourceNotFound) {
			log.Printf("projection not found")
			return
		}
	}
	// endregion updateNotFound
}

func ListAll(client *kurrent.ProjectionClient) {
	// region listAll
	projections, err := client.ListAll(context.Background(), kurrent.GenericProjectionOptions{})

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

func List(client *kurrent.ProjectionClient) {
	// region listContinuous
	projections, err := client.ListContinuous(context.Background(), kurrent.GenericProjectionOptions{})

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

func GetStatus(client *kurrent.ProjectionClient) {
	// region getStatus
	projection, err := client.GetStatus(context.Background(), "$by_category", kurrent.GenericProjectionOptions{})

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

func GetState(client *kurrent.ProjectionClient) {
	projectionName := ""
	// region getState
	type Foobar struct {
		Count int64
	}

	value, err := client.GetState(context.Background(), projectionName, kurrent.GetStateProjectionOptions{})

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

func GetResult(client *kurrent.ProjectionClient) {
	projectionName := ""
	// region getResult
	type Baz struct {
		Result int64
	}

	value, err := client.GetResult(context.Background(), projectionName, kurrent.GetResultProjectionOptions{})

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

func RestartSubSystem(client *kurrent.ProjectionClient) {
	// region restartSubsystem
	err := client.RestartSubsystem(context.Background(), kurrent.GenericProjectionOptions{})

	if err != nil {
		panic(err)
	}
	// endregion restartSubsystem
}
