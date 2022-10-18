package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "time"

    "github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func errHndlr(err error) {
    if err != nil {
        fmt.Println("error:", err)
        os.Exit(1)
    }
}

func worker(id int, jobs <-chan int, results chan<- int, client *redis.ClusterClient, keyPrefix string) {
    for j := range jobs {
        u := fmt.Sprintf("%s:%d", keyPrefix, j)
        _, err := client.HMSet(ctx, u, map[string]interface{}{
            "key1":  u,
            "key2":  u,
            "key3":  u,
            "key4":  u,
            "key5":  u,
            "key6":  u,
            "key7":  u,
            "key8":  u,
            "key9":  u,
            "key10": u,
            "key11": u,
            "key12": u,
            "key13": u,
            "key14": u,
            "key15": u,
            "key16": u,
            "key17": u,
            "key18": u,
            "key19": u,
            "key20": u,
        }).Result()
        errHndlr(err)
        results <- j
    }
    client.Close()
}

func main() {
    redisHost := flag.String("host", "localhost", "Redis Host")
    redisPort := flag.Int("port", 6379, "Redis Port")
    redisPassword := flag.String("password", "", "RedisPassword")
    messageCount := flag.Int("message_count", 1000000, "run this man times")
    threadCount := flag.Int("threadcount", 10, "run this man threads")
    keyPrefix := flag.String("key-prefix", "DBFILLER", "The Key prefix required")
    flag.Parse()

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

    jobs := make(chan int, *messageCount)
    results := make(chan int, *messageCount)

    for w := 0; w <= *threadCount; w++ {
        go worker(w, jobs, results, client, *keyPrefix)
    }

    for j := 0; j <= *messageCount-1; j++ {
        jobs <- j
    }

    // Finally we collect all the results of the work.
    for a := 0; a <= *messageCount-1; a++ {
        <-results
    }
    close(jobs)
    os.Exit(0)

}
