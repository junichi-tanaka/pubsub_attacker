package main

import (
    "log"
    "os"

    "context"
    "net/http"

    "cloud.google.com/go/pubsub"
    "github.com/kelseyhightower/envconfig"
    "google.golang.org/api/option"
)

func main() {
    os.Exit(_main(os.Args[1:]))
}

type envConfig struct {
    CredentialJsonPath string `envconfig:"CREDENTIAL_JSON_PATH" default:"jsonpath"`
    TopicName          string `envconfig:"TOPIC_NAME" default:"my-topic"`
    Port               string `envconfig:"PORT" default:"8000"`
}

func _main(args []string) int {
    log.Printf("start cloud pusbus client")
    var envs envConfig
    err := envconfig.Process("", &envs)
    if err != nil {
        log.Printf("failed to process env vars: %v", err)
        return 1
    }

    log.Printf("environment: %v", envs)

    c := new(cloudHandle)
    ctx := context.Background()
    c.client, err = pubsub.NewClient(ctx, "project-id", option.WithCredentialsFile(envs.CredentialJsonPath))
    c.topic = c.client.Topic(envs.TopicName)
    if err != nil {
        log.Printf("failed to create pubsub client")
        return 1
    }
    http.HandleFunc("/load_test", c.handler)

    log.Printf("server listening on %s", envs.Port)
    err = http.ListenAndServe(":"+envs.Port, nil)
    if err != nil {
        log.Printf("failed to listen server: %v", err)
        return 1
    }
    return 0
}

type cloudHandle struct {
    client *pubsub.Client
    topic  *pubsub.Topic
}

func (c cloudHandle) handler(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    v := r.FormValue("msg")
    // not handle error
    _ := c.topic.Publish(ctx, &pubsub.Message{Data: []byte(v)})
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(v))
    return
}
