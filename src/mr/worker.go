/**
* TODO v2.错误容忍（文件操作）
 */

package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const tmpdir = "./"

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// rpc master for job
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		if ret := call("Master.Deploytask", &args, &reply); !ret {
			fmt.Println("rpc call failed")
			return
		} else {
			fmt.Printf("[Tasktype: %d], [NMap: %d], [NReduce: %d], [MapTaskNumber: %d], [Filename: %s], [ReduceTaskNumber: %d]\n",
				reply.Tasktype, reply.NMap, reply.NReduce, reply.MapTaskNumber,
				reply.Filename, reply.ReduceTaskNumber)
		}

		if reply.Tasktype == 0 {
			go func(rr *WorkerReply) {
				file, err := os.Open(rr.Filename)
				if err != nil {
					log.Fatalf("cannot open %v", rr.Filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", rr.Filename)
				}
				file.Close()

				// fmt.Println("content ...\n", string(content))
				// time.Sleep(time.Second * 3)
				kv := mapf(rr.Filename, string(content))
				sort.Sort(ByKey(kv))

				files, err := _initFileList(rr.NReduce, tmpdir, fmt.Sprintf("mr-%d-", rr.MapTaskNumber))
				if err != nil {
					log.Fatalf("cannot create tmpfile %v", err)
				}

				// fmt.Println("kv result...\n", kv)
				// time.Sleep(time.Second * 10)
				for j := 0; j < len(kv); j++ {
					//fmt.Println(kv[j].Key)
					index := ihash(kv[j].Key) % rr.NReduce
					fmt.Fprintf(files[index], "%v %v\n", kv[j].Key, kv[j].Value)
				}

				// atomically replace
				_renameFileList(files, fmt.Sprintf("mr-%d-", rr.MapTaskNumber))
				fmt.Printf("maptask %d has fininshed\n", rr.MapTaskNumber)
				call("Master.Mapfinshed", &WorkerArgs{MapTaskNumber: rr.MapTaskNumber}, &WorkerReply{})
			}(&reply)
		} else if reply.Tasktype == 1 {
			go func() {
				mrs, err := _openFileList(reply.NMap, tmpdir+"/mr-", reply.ReduceTaskNumber)
				if err != nil {
					log.Panic(err)
				}
				out, err := os.Create(fmt.Sprintf(tmpdir+"/mr-out-%d", reply.ReduceTaskNumber))
				if err != nil {
					log.Panic(err)
				}

				// TODO goroutine 并发
				kva := []KeyValue{}
				for _, mr := range mrs {
					filescanner := bufio.NewScanner(mr)

					for filescanner.Scan() {
						str := filescanner.Text()
						tmp := strings.Split(str, " ")
						// fmt.Println(str, tmp)
						// time.Sleep(time.Second * 2)
						kv := KeyValue{
							Key:   tmp[0],
							Value: tmp[1],
						}
						kva = append(kva, kv)
					}
				}

				sort.Sort(ByKey(kva))
				i, j := 0, 0
				for i < len(kva) {
					values := make([]string, 0)

					for j = i; j < len(kva) && kva[i].Key == kva[j].Key; j++ {
						values = append(values, kva[j].Value)
					}

					output := reducef(kva[i].Key, values)
					fmt.Fprintf(out, "%v %v\n", kva[i].Key, output)
					i = j
				}

				fmt.Printf("reducetask %d has fininshed\n", reply.ReduceTaskNumber)
				call("Master.Reducefinshed", &WorkerArgs{ReduceTaskNumber: reply.ReduceTaskNumber}, &WorkerReply{})
			}()
		} else if reply.Tasktype == 2 {
			//fmt.Println("sleep for waiting")
			time.Sleep(time.Second * 1)
		} else {
			break
		}
	}

	// periodically ask master for task
}

func _openFileList(cnt int, prefix string, suffix int) ([]*os.File, error) {
	fileArray := make([]*os.File, cnt)

	for i := 0; i < cnt; i++ {
		file, err := os.Open(fmt.Sprintf(prefix+"%d-%d", i, suffix))
		if err != nil {
			return nil, err
		}

		fileArray[i] = file
	}
	return fileArray, nil
}

func _initFileList(cnt int, dir, prefix string) ([]*os.File, error) {
	fileArray := make([]*os.File, cnt)

	s, err := os.Stat(dir)
	if os.IsNotExist(err) || !s.IsDir() {
		os.Mkdir(dir, os.ModePerm)
	}

	for i := 0; i < cnt; i++ {
		//file, err := os.Create(fmt.Sprintf(prefix+"%d", i))
		file, err := os.CreateTemp(dir, fmt.Sprintf(prefix+"%d"+".*", i))
		if err != nil {
			return nil, err
		}

		fileArray[i] = file
	}
	return fileArray, nil
}

func _renameFileList(files []*os.File, newname string) {
	for k, v := range files {
		v.Sync()
		//fmt.Sprintf(newname+"%d", k)
		err := os.Rename(v.Name(), fmt.Sprintf("%s/"+newname+"%d", filepath.Dir(v.Name()), k))
		if err != nil {
			fmt.Printf("rename failed, err: %v", err)
			os.RemoveAll(v.Name())
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
