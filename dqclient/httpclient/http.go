package httpclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"../dqclient"
)

var (
	bodyStr    string
	cmdMessage *CmdMessage
	err        error
	ret        string
)

type Http struct {
	Dqclient     *dqclient.DqClient
	Srv          *http.Server
	ExitHttpChan chan bool
}

//{‘command’:’add’, ’topic’:’xxx’, ‘id’: ‘xxx’, ‘delay’: 30, ’TTR’: 60, ‘body’:‘xxx'}
type CmdMessage struct {
	Cmd      string `json:"cmd"`
	Topic    string `json:"topic"`
	Jobid    string `json:"jobid"`
	Name     string `json:"name"`
	Data     string `json:"data"`
	Exectime int64  `json:"exectime"`
	Ttr      int64  `json:"ttr"`
}

type JsonRet struct {
	Code string `json:"code"`
	Info string `json:"info"`
	Data string `json:"data"`
}

func RetJson(code string, info string, data string) string {
	jsonRet := JsonRet{
		Code: code,
		Info: info,
		Data: data,
	}
	ret, _ := json.Marshal(jsonRet)
	return string(ret)
}

func (p *Http) checkBodyValidJson(body []byte) (*CmdMessage, error) {
	err = json.Unmarshal(body, cmdMessage)
	if err != nil {
		return cmdMessage, errors.New("参数不正确，请检查参数中是否有合法的cmd和payload参数")
	}
	return cmdMessage, nil
}

func (p *Http) sayHelloName(w http.ResponseWriter, r *http.Request) {

	body, _ := ioutil.ReadAll(r.Body)
	bodyStr = string(body)
	defer r.Body.Close()
	fmt.Fprintf(w, "Hello "+bodyStr)
}

func (p *Http) addJob(w http.ResponseWriter, r *http.Request) {

	body, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	bodyStr = string(body)
	cmdMessage = &CmdMessage{}
	err = json.Unmarshal(body, cmdMessage)

	if err != nil {
		ret = RetJson("1", "提交参数不正确！"+err.Error(), bodyStr)

	} else {
		//(dqcli *DqClient) AddJob(jobid string, name string, topic string, data string, exectime int64) (string, error) {

		ret1, err1 := p.Dqclient.AddJob(cmdMessage.Jobid, cmdMessage.Name, cmdMessage.Topic, cmdMessage.Data, cmdMessage.Exectime)
		if err1 != nil {
			ret = RetJson("1", "提交参数不正确！"+err1.Error(), ret1)
		} else {
			ret = RetJson("0", "添加任务成功！", ret1)
		}

		fmt.Println(cmdMessage)
	}

	fmt.Fprintf(w, ret)
}

func (p *Http) popJob(w http.ResponseWriter, r *http.Request) {

	body, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	bodyStr = string(body)

	cmdMessage = &CmdMessage{}
	err = json.Unmarshal(body, cmdMessage)

	if err != nil {
		ret = RetJson("1", "提交参数不正确！"+err.Error(), bodyStr)

	} else {

		ret1, err1 := p.Dqclient.Pop(cmdMessage.Topic)
		if err1 != nil {
			ret = RetJson("1", "取任务出错！"+err1.Error(), ret1)
		} else {
			if ret1 == "" {
				ret = RetJson("1", "取任务出错,当前Topic下暂无Ready job！", ret1)
			} else {
				ret = RetJson("0", "取出任务成功！", ret1)
			}

		}

		fmt.Println("获取到的参数", cmdMessage)
	}

	fmt.Fprintf(w, ret)

}

func (p *Http) finishJob(w http.ResponseWriter, r *http.Request) {

	body, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	bodyStr = string(body)

	cmdMessage = &CmdMessage{}
	err = json.Unmarshal(body, cmdMessage)

	if err != nil {
		ret = RetJson("1", "提交参数不正确！"+err.Error(), bodyStr)

	} else {

		ret1, err1 := p.Dqclient.FinishJob(cmdMessage.Jobid)
		if err1 != nil {
			ret = RetJson("1", "标识完成任务出错！"+err1.Error(), ret1)
		} else {
			ret = RetJson("0", "标识完成任务成功！", ret1)
		}

		fmt.Println("获取到的参数", cmdMessage)
	}

	fmt.Fprintf(w, ret)

}

func (p *Http) getJobInfo(w http.ResponseWriter, r *http.Request) {

	body, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	bodyStr = string(body)

	cmdMessage = &CmdMessage{}
	err = json.Unmarshal(body, cmdMessage)

	if err != nil {
		ret = RetJson("1", "提交参数不正确！"+err.Error(), bodyStr)

	} else {

		ret1, err1 := p.Dqclient.GetJobInfo(cmdMessage.Jobid)
		if err1 != nil {
			ret = RetJson("1", "获取任务信息出错！"+err1.Error(), ret1)
		} else {
			ret = RetJson("0", "获取任务成功成功！", ret1)
		}

		fmt.Println("获取到的参数", cmdMessage)
	}

	fmt.Fprintf(w, ret)
}

/*
*	获取服务状态--目前仅返回redis状态
 */

func (p *Http) ping(w http.ResponseWriter, r *http.Request) {
	ret1, err1 := p.Dqclient.Ping()

	if err1 != nil {
		ret = RetJson("1", time.Now().Format("2006-01-02 15:04:05")+"当前状态出错！"+err1.Error(), ret1)
	} else {
		ret = RetJson("0", time.Now().Format("2006-01-02 15:04:05")+"当前状态正常！", ret1)
	}

	fmt.Fprintf(w, ret)
}

func (p *Http) Stop() {
	p.ExitHttpChan <- true
}

func (p *Http) Serve() {

	p.ExitHttpChan = make(chan bool, 1)

	http.HandleFunc("/", p.sayHelloName)
	http.HandleFunc("/delayq/ping", p.ping)

	http.HandleFunc("/delayq/job/info", p.getJobInfo)
	http.HandleFunc("/delayq/job/add", p.addJob)
	http.HandleFunc("/delayq/job/pop", p.popJob)
	http.HandleFunc("/delayq/job/finish", p.finishJob)

	port := "9090"
	p.Srv = &http.Server{Addr: ":" + port, Handler: http.DefaultServeMux}

	fmt.Println("Http Server is Starting on port ", port)

	go func() {
		err := p.Srv.ListenAndServe()
		if err != nil {
			log.Println("ListenAndServe: ", err)
		}
	}()

	go func() {
		<-p.ExitHttpChan
		p.Srv.Close()
		fmt.Println("http服务器收到退出信息, 已退出")
	}()

	time.Sleep(time.Second * 100)

}
