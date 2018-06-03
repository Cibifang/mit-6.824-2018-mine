package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	kvMap := make(map[string][]string)

	/* Read intermediate files */
	for m := 0; m < nMap; m++ {
		fileName := reduceName(jobName, m, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf(
				"doReduce: open file `%s` failed, err: `%v`", fileName, err)
			return
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Printf(
					"doReduce: decode feil `%s` failed, err: `%v`",
					fileName, err)
				file.Close()
				return
			}

			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}

		file.Close()
	}

	/* Sort keys */
	kList := make([]string, 0)
	for key := range kvMap {
		kList = append(kList, key)
	}
	sort.Strings(kList)

	/* Create/Open output file */
	outF, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Printf(
			"doReduce: open outFile `%s` failed, err: `%v`", outFile, err)
		return
	}

	/* Write to output file */
	enc := json.NewEncoder(outF)
	for _, key := range kList {
		err := enc.Encode(KeyValue{key, reduceF(key, kvMap[key])})
		if err != nil {
			log.Printf(
				"doReduce: Error `%v` when encode `%v` in outfile `%s`",
				err, KeyValue{key, reduceF(key, kvMap[key])}, outFile)
			outF.Close()
			return
		}
	}

	log.Printf("doReduce: out file `%s` write success", outFile)
	outF.Close()
}
