package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/KablamoOSS/exportexample/athena"
	"github.com/aws/aws-sdk-go/aws/session"
)

var poll time.Duration
var timeout time.Duration
var skipHeaderRow bool

func init() {
	const (
		defaultPoll    = 1 * time.Second
		defaultTimeout = 5 * time.Second
	)

	flag.DurationVar(&poll, "poll", defaultPoll, "specify polling interval in milliseconds")
	flag.DurationVar(&timeout, "timeout", defaultTimeout, "specify timeout in milliseconds")
	flag.BoolVar(&skipHeaderRow, "skip-header-row", false, "skip header row containing column names")
}

func main() {
	flag.Usage = func() {
		usage := fmt.Sprintf("%s [FLAG...] DATABASE_NAME QUERY_STATEMENT S3_OUTPUT_URL\n\nFlags:\n\n", path.Base(os.Args[0]))
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}

	flag.Parse()

	remaining := flag.Args()

	if len(remaining) < 3 {
		flag.Usage()
		os.Exit(1)
	}

	database := remaining[0]
	queryStatement := remaining[1]
	s3url := remaining[2]

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		fmt.Fprintln(os.Stderr, "error: unable to create AWS session:", err)
		os.Exit(1)
	}

	if sess.Config.Region == nil || *sess.Config.Region == "" {
		fmt.Fprintln(os.Stderr, "error: AWS region unknown, specify AWS_REGION and/or AWS_PROFILE environment variable")
		os.Exit(1)
	}

	client, err := athena.NewClient(sess)

	if err != nil {
		fmt.Fprintln(os.Stderr, "error: unable to create Athena client:", err)
		os.Exit(1)
	}

	q, err := client.DoQuery(database, queryStatement, s3url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to create Athena query:", err)
		os.Exit(1)
	}

	deadline := make(chan time.Time)

	go func() {
		deadline <- <-time.After(timeout)
	}()

	var r athena.Result
	var qs athena.QueryStatus

	for {
		select {
		case <-deadline:
			fmt.Fprintf(os.Stderr, "deadline reached (%s)\n", timeout)
			os.Exit(1)
		default:
			break
		}

		var done bool
		qs, err = q.Status()

		if err != nil {
			fmt.Fprintln(os.Stderr, "error getting query status:", err)
			os.Exit(1)
		}

		if done {
			r, err = q.Result()

			if err != nil {
				fmt.Fprintln(os.Stderr, "error getting query result:", err)
				os.Exit(1)
			}

			break
		}

		time.Sleep(poll)
	}

	if skipHeaderRow {
		r.Rows = r.Rows[1:]
	}

	out := struct {
		OutputLocation string `json:"s3_output_location"`
		athena.Result
	}{qs.OutputLocation, r}

	j, _ := json.Marshal(out)
	fmt.Println(string(j))
}
