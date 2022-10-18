package main

import (
    "context"
    "encoding/csv"
    "flag"
    "fmt"
    "log"
    "math/rand"
    "os"
    "time"

    "github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func main() {

    redisHost := flag.String("host", "localhost", "Redis Host")
    redisPort := flag.Int("port", 6379, "Redis Port")
    redisPassword := flag.String("password", "", "RedisPassword")
    messageCount := flag.Int("message_count", 1000000, "run this man times")
    sampleCount := flag.Int("sample_count", 1000, "fetch this many records")
    keyPrefix := flag.String("key-prefix", "DBFILLER", "The Key prefix required")
    outCSV := flag.String("csv-file", "results.csv", "CSV file to store results")

    client := redis.NewClusterClient(&redis.ClusterOptions{
        Addrs:        []string{fmt.Sprintf("%s:%d", *redisHost, *redisPort)},
        MaxRedirects: 3,
        Password:     *redisPassword,
        PoolSize:     8,
        MinIdleConns: 5,
        PoolTimeout:  0,
        IdleTimeout:  20 * time.Second,
        DialTimeout:  2 * time.Second,
    })

    flag.Parse()

    f, err := os.Create(*outCSV)
    defer f.Close()

    if err != nil {

        log.Fatalln("failed to open file", err)
    }
    var list = make([]int, *messageCount)
    for j := 0; j < *messageCount; j++ {
        list[j] = j + 1
    }
    rand.Shuffle(len(list), func(i, j int) {
        list[i], list[j] = list[j], list[i]
    })

    w := csv.NewWriter(f)
    defer w.Flush()

    if err := w.Write([]string{"key", "initial", "secondary"}); err != nil {
        log.Fatalln("error writing record to file", err)
    }

    for i := 0; i <= *sampleCount; i++ {
        x := list[i]
        startTimeInit := time.Now()
        client.HGetAll(ctx, fmt.Sprintf("%s:%d", *keyPrefix, x)).Result()
        endTimeInit := time.Since(startTimeInit).Microseconds()
        startTimeFinal := time.Now()
        client.HGetAll(ctx, fmt.Sprintf("%s:%d", *keyPrefix, x)).Result()
        endTimeFinal := time.Since(startTimeFinal).Microseconds()
        // We throw out the initial one to not skew the results
        if i > 0 {
            if err := w.Write([]string{
                fmt.Sprintf("%s:%d", *keyPrefix, x),
                fmt.Sprintf("%d", endTimeInit),
                fmt.Sprintf("%d", endTimeFinal),
            }); err != nil {
                log.Fatalln("error writing record to file", err)
            }
        }
    }

}
