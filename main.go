package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"

	"notifyCollectDB/config"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"

	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"github.com/influxdata/telegraf/plugins/serializers"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/godror/godror"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"xorm.io/xorm"
)

func main() {
	log.SetOutput(&lumberjack.Logger{
		Filename:   "./log/notifyCollectDB.log",
		MaxSize:    10,
		MaxBackups: 10,
		MaxAge:     28,
		Compress:   false,
	})

	cfg := config.NewConfig()
	err := cfg.LoadConfig("notifyCollectDB.conf")
	if err != nil {
		log.Printf("Failed to load config file: %v\n", err)
		return
	}
	if len(cfg.Cfg.Items) == 0 {
		log.Println("Nothing observe item")
		return
	}

	serializer := serializers.NewInfluxSerializer()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	done := make(chan string)
	go func() {
		for {
			var sc = bufio.NewScanner(os.Stdin)
			if sc.Scan() {
				line := sc.Text()
				log.Printf("Input: [%s]\n", line)

				parser := influx.Parser{}
				parser.Init()
				metric, err := parser.ParseLine(line)
				if err != nil {
					log.Printf("Parse Error: %V [%s]\n", err, line)
					continue
				}

				var kk []*regexp.Regexp
				var vv []string
				for _, v := range metric.FieldList() {
					rep := regexp.MustCompile(fmt.Sprintf("@%s", v.Key))
					kk = append(kk, rep)
					vv = append(vv, fmt.Sprintf("%v", v.Value))
				}

				driverName := cfg.Cfg.Driver
				connStr := cfg.Cfg.ConnectionString
				engine, err := xorm.NewEngine(driverName, connStr)
				if err != nil {
					log.Printf("Failed to open target DB: %v\n", err)
					continue
				}
				defer engine.Close()

				for i := 0; i < len(cfg.Cfg.Items); i++ {
					id := cfg.Cfg.Items[i].ID
					strFmtSQL := cfg.Cfg.Items[i].SqlTemplate
					tags := cfg.Cfg.Items[i].Tags
					mapTags := make(map[string]string)
					for _, v := range tags {
						mapTags[v] = ""
					}
					excludes := cfg.Cfg.Items[i].ExcludeColumns
					mapExcludes := make(map[string]string)
					for _, v := range excludes {
						mapExcludes[v] = ""
					}

					// Replace
					for i, v := range kk {
						strFmtSQL = v.ReplaceAllString(strFmtSQL, vv[i])
					}
					strSQL := strFmtSQL

					results, err := engine.QueryInterface(strSQL)
					if err != nil {
						log.Printf("Failed to query: %v [%s]\n", err, strSQL)
						continue
					}
					for _, vs := range results {
						tags := make(map[string]string)
						field := make(map[string]interface{})
						for k, v := range vs {
							// Check NOT NULL
							if len(k) == 0 || v == nil {
								continue
							}

							if _, ok := mapTags[k]; ok {
								strValue := ""
								if vv, ok := v.(string); ok {
									strValue = vv
								} else {
									switch t := v.(type) {
									case []uint8:
										strValue = string(t)
									case int32:
										strValue = fmt.Sprint(int(t))
									case float64:
										strValue = fmt.Sprint(float64(t))
									default:
										log.Printf("default type: %v\n", t)
										strValue = fmt.Sprintf("%v", t)
									}
								}
								tags[k] = strValue
							} else if _, ok := mapExcludes[k]; !ok {
								if vv, ok := v.(string); ok {
									field[k] = vv
								} else {
									switch t := v.(type) {
									case []uint8:
										field[k] = string(t)
									case int32:
										field[k] = int(t)
									case float64:
										field[k] = float64(t)
									default:
										log.Printf("default type: %v\n", t)
										field[k] = fmt.Sprintf("%v", t)
									}
								}
							}
						}

						mtx := metric.Copy()
						mtx.SetName(id)
						for _, v := range metric.FieldList() {
							mtx.RemoveField(v.Key)
						}

						for k, v := range tags {
							mtx.AddTag(k, v)
						}
						for k, v := range field {
							mtx.AddField(k, v)
						}

						b, err := serializer.Serialize(mtx)
						if err != nil {
							log.Printf("ERR %v\n", err)
							continue
						}
						outline := string(b)
						log.Printf("output %s\n", outline)
						fmt.Fprint(os.Stdout, outline)
					}
				}
			} else {
				done <- "done"
			}
			if sc.Err() != nil {
				done <- "done"
				break
			}
		}
	}()

	select {
	case <-quit:
	case <-done:
	}
}
