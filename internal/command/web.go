// Copyright (c) 2020 tickstep.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package command

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tickstep/aliyunpan-api/aliyunpan"
	"github.com/tickstep/aliyunpan-api/aliyunpan/apierror"
	"github.com/tickstep/aliyunpan/internal/config"
	"github.com/tickstep/aliyunpan/internal/file/uploader"
	"github.com/tickstep/aliyunpan/internal/functions/panupload"
	"github.com/tickstep/aliyunpan/internal/localfile"
	"github.com/tickstep/aliyunpan/internal/log"
	"github.com/tickstep/aliyunpan/internal/taskframework"
	"github.com/tickstep/library-go/requester/rio/speeds"
	"github.com/urfave/cli"
)

const (
	webDefaultAddr      = "0.0.0.0:8080"
	webStatusReceiving  = "receiving"
	webStatusQueued     = "queued"
	webStatusUploading  = "uploading"
	webStatusSuccess    = "success"
	webStatusFailed     = "failed"
	webCopyBufferSize   = 1024 * 1024
	webUploadTaskRetain = 200
)

type webServer struct {
	addr      string
	rootPath  string
	driveId   string
	user      *config.PanUser
	tempDir   string
	chunkDir  string
	queue     chan *webUploadTask
	tasks     map[string]*webUploadTask
	tasksMu   sync.RWMutex
	nextID    int64
	uploadDB  *panupload.UploadingDatabase
	folderMu  sync.Mutex
	speeds    *speeds.Speeds
	recorder  *log.FileRecorder
	closeOnce sync.Once
	chunksMu  sync.Mutex
}

type webUploadTask struct {
	ID             string
	FileName       string
	TargetPath     string
	Status         string
	Size           int64
	Received       int64
	Uploaded       int64
	Speed          int64
	Error          string
	CreatedAt      string
	UpdatedAt      string
	tempPath       string
	savePath       string
	chunkDir       string
	chunkTotal     int
	chunkReceived  map[int]bool
	lastReceivedAt time.Time
	lastReceived   int64
	mu             sync.RWMutex
}

type webUploadTaskSnapshot struct {
	ID         string `json:"id"`
	FileName   string `json:"fileName"`
	TargetPath string `json:"targetPath"`
	Status     string `json:"status"`
	Size       int64  `json:"size"`
	Received   int64  `json:"received"`
	Uploaded   int64  `json:"uploaded"`
	Speed      int64  `json:"speed"`
	Error      string `json:"error,omitempty"`
	CreatedAt  string `json:"createdAt"`
	UpdatedAt  string `json:"updatedAt"`
}

type webFileInfo struct {
	FileID    string `json:"fileId"`
	FileName  string `json:"fileName"`
	FileSize  int64  `json:"fileSize"`
	FileType  string `json:"fileType"`
	IsFolder  bool   `json:"isFolder"`
	UpdatedAt string `json:"updatedAt"`
	CreatedAt string `json:"createdAt"`
}

type webChunkInitRequest struct {
	FileName    string `json:"fileName"`
	Size        int64  `json:"size"`
	ChunkSize   int64  `json:"chunkSize"`
	ChunkTotal  int    `json:"chunkTotal"`
	Fingerprint string `json:"fingerprint"`
	Path        string `json:"path"`
}

func CmdWeb() cli.Command {
	return cli.Command{
		Name:      "web",
		Usage:     "启动Web上传模式",
		UsageText: "aliyunpan web -path <云盘目录> [-addr 0.0.0.0:8080]",
		Description: `启动一个简单Web页面，只允许浏览和上传到指定云盘目录及其子目录。

示例:
  aliyunpan web -path /上传目录
  aliyunpan web -path /上传目录 -addr 0.0.0.0:8080`,
		Category: "阿里云盘",
		Before:   ReloadConfigFunc,
		Action: func(c *cli.Context) error {
			rootPath := strings.TrimSpace(c.String("path"))
			if rootPath == "" {
				fmt.Println("错误: web 模式必须指定 -path 云盘目录")
				return nil
			}
			addr := strings.TrimSpace(c.String("addr"))
			if addr == "" {
				addr = webDefaultAddr
			}

			activeUser := GetActiveUser()
			if activeUser == nil || activeUser.UserId == "" {
				return ErrNotLogined
			}
			driveId := activeUser.ActiveDriveId
			if c.IsSet("driveId") {
				driveId = c.String("driveId")
			}

			server, err := newWebServer(addr, rootPath, driveId, activeUser)
			if err != nil {
				fmt.Printf("启动 web 模式失败: %s\n", err)
				return nil
			}
			defer server.Close()
			return server.ListenAndServe()
		},
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "path",
				Usage: "允许Web浏览和上传的云盘目录，必填",
			},
			cli.StringFlag{
				Name:  "addr",
				Usage: "Web服务监听地址",
				Value: webDefaultAddr,
			},
			cli.StringFlag{
				Name:  "driveId",
				Usage: "网盘ID，默认使用当前激活网盘",
				Value: "",
			},
		},
	}
}

func newWebServer(addr, rootPath, driveId string, user *config.PanUser) (*webServer, error) {
	rootPath = user.PathJoin(driveId, rootPath)
	if rootPath == "." || rootPath == "" {
		rootPath = "/"
	}
	info, err := user.PanClient().OpenapiPanClient().FileInfoByPath(driveId, rootPath)
	if err != nil {
		return nil, fmt.Errorf("指定云盘目录不可访问: %w", err)
	}
	if info == nil || !info.IsFolder() {
		return nil, fmt.Errorf("指定 -path 不是云盘文件夹: %s", rootPath)
	}
	tempDir, err1 := os.MkdirTemp("", "aliyunpan-web-upload-*")
	if err1 != nil {
		return nil, err1
	}
	uploadDB, err2 := panupload.NewUploadingDatabase()
	if err2 != nil {
		os.RemoveAll(tempDir)
		return nil, err2
	}

	s := &webServer{
		addr:     addr,
		rootPath: rootPath,
		driveId:  driveId,
		user:     user,
		tempDir:  tempDir,
		chunkDir: filepath.Join(tempDir, "chunks"),
		queue:    make(chan *webUploadTask, 1024),
		tasks:    map[string]*webUploadTask{},
		uploadDB: uploadDB,
		speeds:   &speeds.Speeds{},
		recorder: log.NewFileRecorder(config.GetLogDir() + "/upload_file_records.csv"),
	}
	if err := os.MkdirAll(s.chunkDir, 0700); err != nil {
		uploadDB.Close()
		os.RemoveAll(tempDir)
		return nil, err
	}
	parallel := config.Config.MaxUploadParallel
	if parallel <= 0 {
		parallel = config.DefaultFileUploadParallelNum
	}
	if parallel < 1 {
		parallel = 1
	}
	for i := 0; i < parallel; i++ {
		go s.uploadWorker()
	}
	return s, nil
}

func (s *webServer) Close() {
	s.closeOnce.Do(func() {
		if s.uploadDB != nil {
			s.uploadDB.Close()
		}
		if s.tempDir != "" {
			os.RemoveAll(s.tempDir)
		}
	})
}

func (s *webServer) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.Handle("/assets/web/material-icons/", http.StripPrefix("/assets/web/material-icons/", http.FileServer(http.Dir("assets/web/material-icons"))))
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/files", s.handleFiles)
	mux.HandleFunc("/api/upload", s.handleUpload)
	mux.HandleFunc("/api/upload/init", s.handleChunkInit)
	mux.HandleFunc("/api/upload/chunk", s.handleChunkUpload)
	mux.HandleFunc("/api/upload/complete", s.handleChunkComplete)
	mux.HandleFunc("/api/uploads", s.handleUploads)
	mux.HandleFunc("/api/uploads/history", s.handleClearUploadHistory)
	mux.HandleFunc("/api/uploads/", s.handleUploadTask)
	mux.HandleFunc("/api/ws", s.handleWebSocket)

	fmt.Printf("Web模式已启动: http://%s\n", s.addr)
	fmt.Printf("限制目录: %s\n", s.rootPath)
	fmt.Printf("提示: 当前模式无鉴权，请只在可信网络中使用。\n")
	return http.ListenAndServe(s.addr, mux)
}

func (s *webServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = webIndexTemplate.Execute(w, map[string]string{
		"RootPath": s.rootPath,
	})
}

func (s *webServer) handleFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rel := r.URL.Query().Get("path")
	targetPath, err := s.resolvePanPath(rel)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	targetPathInfo, apierr := s.user.PanClient().OpenapiPanClient().FileInfoByPath(s.driveId, targetPath)
	if apierr != nil {
		status := http.StatusInternalServerError
		if apierr.Code == apierror.ApiCodeFileNotFoundCode {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]interface{}{"error": apierr.Error()})
		return
	}
	if targetPathInfo == nil || !targetPathInfo.IsFolder() {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "目标路径不是文件夹"})
		return
	}
	fileResult, err1 := s.user.PanClient().OpenapiPanClient().FileListGetAll(&aliyunpan.FileListParam{
		ParentFileId:   targetPathInfo.FileId,
		DriveId:        s.driveId,
		OrderBy:        aliyunpan.FileOrderByUpdatedAt,
		OrderDirection: aliyunpan.FileOrderDirectionDesc,
	}, 200)
	if err1 != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err1.Error()})
		return
	}
	files := make([]webFileInfo, 0, len(fileResult))
	for _, file := range fileResult {
		files = append(files, webFileInfo{
			FileID:    file.FileId,
			FileName:  file.FileName,
			FileSize:  file.FileSize,
			FileType:  file.FileExtension,
			IsFolder:  file.IsFolder(),
			UpdatedAt: file.UpdatedAt,
			CreatedAt: file.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"path":  s.relativeDisplayPath(targetPath),
		"files": files,
	})
}

func (s *webServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	targetDir, err := s.resolvePanPath(r.URL.Query().Get("path"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	reader, err := r.MultipartReader()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	tasks := []*webUploadTaskSnapshot{}
	for {
		part, err1 := reader.NextPart()
		if errors.Is(err1, io.EOF) {
			break
		}
		if err1 != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err1.Error()})
			return
		}
		if part.FormName() != "files" && part.FormName() != "file" {
			part.Close()
			continue
		}
		if part.FileName() == "" {
			part.Close()
			continue
		}
		task, err2 := s.receiveUploadPart(part, targetDir)
		part.Close()
		if err2 != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err2.Error()})
			return
		}
		tasks = append(tasks, task.snapshot())
		s.queue <- task
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tasks": tasks})
}

func (s *webServer) handleChunkInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req webChunkInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	targetDir, err := s.resolvePanPath(req.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	fileName := path.Base(strings.ReplaceAll(req.FileName, "\\", "/"))
	if fileName == "." || fileName == "/" || fileName == "" || req.Size < 0 || req.ChunkTotal <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "上传参数非法"})
		return
	}
	uploadID := stableUploadID(req.Fingerprint, targetDir, fileName, req.Size)

	s.chunksMu.Lock()
	task := s.tasks[uploadID]
	if task == nil {
		task = s.newTaskWithID(uploadID, fileName, path.Join(targetDir, fileName))
		task.Size = req.Size
		task.chunkTotal = req.ChunkTotal
		task.chunkReceived = map[int]bool{}
		task.chunkDir = filepath.Join(s.chunkDir, uploadID)
		_ = os.MkdirAll(task.chunkDir, 0700)
	} else if task.chunkReceived == nil {
		task.chunkReceived = map[int]bool{}
	}
	if task.Size == 0 {
		task.Size = req.Size
	}
	task.restoreChunksLocked()
	received := task.receivedChunksLocked()
	s.chunksMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"uploadId": uploadID,
		"task":     task.snapshot(),
		"received": received,
	})
}

func (s *webServer) handleChunkUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	index, err := strconv.Atoi(r.URL.Query().Get("index"))
	if uploadID == "" || err != nil || index < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "分片参数非法"})
		return
	}
	s.tasksMu.RLock()
	task := s.tasks[uploadID]
	s.tasksMu.RUnlock()
	if task == nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"error": "上传任务不存在"})
		return
	}
	if task.chunkTotal <= 0 || index >= task.chunkTotal {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "分片序号非法"})
		return
	}

	s.chunksMu.Lock()
	defer s.chunksMu.Unlock()
	if task.chunkReceived[index] {
		writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "received": task.receivedChunksLocked()})
		return
	}
	task.setStatus(webStatusReceiving, "")
	if err := os.MkdirAll(task.chunkDir, 0700); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	chunkPath := filepath.Join(task.chunkDir, fmt.Sprintf("%08d.part", index))
	tmpPath := chunkPath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	n, copyErr := io.CopyBuffer(file, r.Body, make([]byte, webCopyBufferSize))
	closeErr := file.Close()
	if copyErr != nil {
		os.Remove(tmpPath)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": copyErr.Error()})
		return
	}
	if closeErr != nil {
		os.Remove(tmpPath)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": closeErr.Error()})
		return
	}
	if err = os.Rename(tmpPath, chunkPath); err != nil {
		os.Remove(tmpPath)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	task.chunkReceived[index] = true
	task.addReceived(n)
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "received": task.receivedChunksLocked()})
}

func (s *webServer) handleChunkComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	s.tasksMu.RLock()
	task := s.tasks[uploadID]
	s.tasksMu.RUnlock()
	if task == nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"error": "上传任务不存在"})
		return
	}

	s.chunksMu.Lock()
	if len(task.chunkReceived) != task.chunkTotal {
		received := task.receivedChunksLocked()
		s.chunksMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]interface{}{"error": "分片未上传完成", "received": received})
		return
	}
	tmpFile, err := os.CreateTemp(s.tempDir, "upload-*")
	if err != nil {
		s.chunksMu.Unlock()
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	tmpPath := tmpFile.Name()
	buf := make([]byte, webCopyBufferSize)
	for i := 0; i < task.chunkTotal; i++ {
		partPath := filepath.Join(task.chunkDir, fmt.Sprintf("%08d.part", i))
		partFile, err := os.Open(partPath)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			s.chunksMu.Unlock()
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
			return
		}
		_, err = io.CopyBuffer(tmpFile, partFile, buf)
		partFile.Close()
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			s.chunksMu.Unlock()
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
			return
		}
	}
	if err = tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		s.chunksMu.Unlock()
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	task.tempPath = tmpPath
	task.setQueued()
	os.RemoveAll(task.chunkDir)
	s.chunksMu.Unlock()

	s.queue <- task
	writeJSON(w, http.StatusOK, map[string]interface{}{"task": task.snapshot()})
}

func (s *webServer) receiveUploadPart(part *multipart.Part, targetDir string) (*webUploadTask, error) {
	fileName := path.Base(strings.ReplaceAll(part.FileName(), "\\", "/"))
	if fileName == "." || fileName == "/" || fileName == "" {
		return nil, fmt.Errorf("非法文件名: %s", part.FileName())
	}
	task := s.newTask(fileName, path.Join(targetDir, fileName))
	tmpFile, err := os.CreateTemp(s.tempDir, "upload-*")
	if err != nil {
		task.fail(err)
		return nil, err
	}
	task.tempPath = tmpFile.Name()
	buf := make([]byte, webCopyBufferSize)
	for {
		n, readErr := part.Read(buf)
		if n > 0 {
			if _, err = tmpFile.Write(buf[:n]); err != nil {
				tmpFile.Close()
				task.fail(err)
				return nil, err
			}
			task.addReceived(int64(n))
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			tmpFile.Close()
			task.fail(readErr)
			return nil, readErr
		}
	}
	if err = tmpFile.Close(); err != nil {
		task.fail(err)
		return nil, err
	}
	task.setQueued()
	return task, nil
}

func (s *webServer) handleUploads(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tasks": s.taskSnapshots()})
}

func (s *webServer) handleClearUploadHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	removed := 0
	s.tasksMu.Lock()
	for id, task := range s.tasks {
		status := task.status()
		if status == webStatusSuccess || status == webStatusFailed {
			delete(s.tasks, id)
			removed++
		}
	}
	s.tasksMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]interface{}{"removed": removed, "tasks": s.taskSnapshots()})
}

func (s *webServer) taskSnapshots() []*webUploadTaskSnapshot {
	s.tasksMu.RLock()
	tasks := make([]*webUploadTaskSnapshot, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, t.snapshot())
	}
	s.tasksMu.RUnlock()
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID > tasks[j].ID
	})
	if len(tasks) > webUploadTaskRetain {
		tasks = tasks[:webUploadTaskRetain]
	}
	return tasks
}

func (s *webServer) handleUploadTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/uploads/")
	s.tasksMu.RLock()
	task := s.tasks[id]
	s.tasksMu.RUnlock()
	if task == nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"error": "任务不存在"})
		return
	}
	writeJSON(w, http.StatusOK, task.snapshot())
}

func (s *webServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "upgrade required", http.StatusUpgradeRequired)
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing websocket key", http.StatusBadRequest)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket unsupported", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()

	acceptHash := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(acceptHash[:])
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\n")
	fmt.Fprintf(rw, "Upgrade: websocket\r\n")
	fmt.Fprintf(rw, "Connection: Upgrade\r\n")
	fmt.Fprintf(rw, "Sec-WebSocket-Accept: %s\r\n\r\n", accept)
	if err = rw.Flush(); err != nil {
		return
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		payload, err := json.Marshal(map[string]interface{}{
			"type":  "uploads",
			"tasks": s.taskSnapshots(),
		})
		if err != nil {
			return
		}
		if err = writeWebSocketText(rw, payload); err != nil {
			return
		}
	}
}

func (s *webServer) uploadWorker() {
	for task := range s.queue {
		s.runUploadTask(task)
	}
}

func (s *webServer) runUploadTask(task *webUploadTask) {
	task.setStatus(webStatusUploading, "")
	defer os.Remove(task.tempPath)
	unit := &panupload.UploadTaskUnit{
		LocalFileChecksum: localfile.NewLocalFileEntity(task.tempPath),
		SavePath:          task.savePath,
		DriveId:           s.driveId,
		PanClient:         s.user.PanClient(),
		UploadingDatabase: s.uploadDB,
		FolderCreateMutex: &s.folderMu,
		Parallel:          1,
		NoRapidUpload:     false,
		BlockSize:         int64(10240 * 1024),
		UploadStatistic:   &panupload.UploadStatistic{},
		ShowProgress:      false,
		IsOverwrite:       false,
		IsSkipSameName:    false,
		GlobalSpeedsStat:  s.speeds,
		FileRecorder:      s.recorder,
		OnUploadStatus: func(status uploader.Status) {
			task.setUploaded(status.Uploaded(), status.SpeedsPerSecond())
		},
		OnTaskSuccess: func() {
			task.setUploaded(task.Size, 0)
			task.setStatus(webStatusSuccess, "")
			s.user.DeleteCache(GetAllPathFolderByPath(path.Dir(task.savePath)))
		},
		OnTaskFailed: func(message string, err error) {
			if err != nil {
				if message != "" {
					message += ": "
				}
				message += err.Error()
			}
			if message == "" {
				message = "上传失败"
			}
			task.setStatus(webStatusFailed, message)
		},
	}
	executor := taskframework.NewTaskExecutor()
	executor.SetParallel(1)
	executor.Append(unit, DefaultUploadMaxRetry)
	executor.Execute()
	if task.status() == webStatusUploading {
		task.setStatus(webStatusFailed, "上传任务异常结束")
	}
}

func (s *webServer) newTask(fileName, savePath string) *webUploadTask {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	s.nextID++
	return s.newTaskLocked(fmt.Sprintf("%d", s.nextID), fileName, savePath)
}

func (s *webServer) newTaskWithID(id, fileName, savePath string) *webUploadTask {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	if task := s.tasks[id]; task != nil {
		return task
	}
	return s.newTaskLocked(id, fileName, savePath)
}

func (s *webServer) newTaskLocked(id, fileName, savePath string) *webUploadTask {
	now := time.Now()
	task := &webUploadTask{
		ID:             id,
		FileName:       fileName,
		TargetPath:     s.relativeDisplayPath(path.Dir(savePath)),
		Status:         webStatusReceiving,
		CreatedAt:      now.Format(time.RFC3339),
		UpdatedAt:      now.Format(time.RFC3339),
		savePath:       savePath,
		lastReceivedAt: now,
	}
	s.tasks[task.ID] = task
	return task
}

func (t *webUploadTask) snapshot() *webUploadTaskSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return &webUploadTaskSnapshot{
		ID:         t.ID,
		FileName:   t.FileName,
		TargetPath: t.TargetPath,
		Status:     t.Status,
		Size:       t.Size,
		Received:   t.Received,
		Uploaded:   t.Uploaded,
		Speed:      t.Speed,
		Error:      t.Error,
		CreatedAt:  t.CreatedAt,
		UpdatedAt:  t.UpdatedAt,
	}
}

func (t *webUploadTask) addReceived(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.Received += n
	t.Size = t.Received
	if elapsed := now.Sub(t.lastReceivedAt).Seconds(); elapsed > 0 {
		t.Speed = int64(float64(t.Received-t.lastReceived) / elapsed)
	}
	t.lastReceivedAt = now
	t.lastReceived = t.Received
	t.UpdatedAt = now.Format(time.RFC3339)
}

func (t *webUploadTask) setQueued() {
	t.setStatus(webStatusQueued, "")
}

func (t *webUploadTask) setUploaded(uploaded, speed int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Uploaded = uploaded
	t.Speed = speed
	t.UpdatedAt = time.Now().Format(time.RFC3339)
}

func (t *webUploadTask) setStatus(status, message string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Status = status
	t.Error = message
	t.UpdatedAt = time.Now().Format(time.RFC3339)
}

func (t *webUploadTask) status() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status
}

func (t *webUploadTask) fail(err error) {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	t.setStatus(webStatusFailed, msg)
}

func (t *webUploadTask) receivedChunksLocked() []int {
	received := make([]int, 0, len(t.chunkReceived))
	for idx := range t.chunkReceived {
		received = append(received, idx)
	}
	sort.Ints(received)
	return received
}

func (t *webUploadTask) restoreChunksLocked() {
	if t.chunkDir == "" || t.chunkTotal <= 0 {
		return
	}
	var receivedBytes int64
	for i := 0; i < t.chunkTotal; i++ {
		partPath := filepath.Join(t.chunkDir, fmt.Sprintf("%08d.part", i))
		if info, err := os.Stat(partPath); err == nil {
			t.chunkReceived[i] = true
			receivedBytes += info.Size()
		}
	}
	if receivedBytes > t.Received {
		t.mu.Lock()
		t.Received = receivedBytes
		t.UpdatedAt = time.Now().Format(time.RFC3339)
		t.mu.Unlock()
	}
}

func stableUploadID(fingerprint, targetDir, fileName string, size int64) string {
	sum := sha1.Sum([]byte(fmt.Sprintf("%s|%s|%s|%d", fingerprint, targetDir, fileName, size)))
	return fmt.Sprintf("%x", sum[:])
}

func (s *webServer) resolvePanPath(rel string) (string, error) {
	rel = strings.ReplaceAll(rel, "\\", "/")
	if strings.ContainsRune(rel, 0) {
		return "", errors.New("路径非法")
	}
	if rel == "" || rel == "/" || rel == "." {
		return s.rootPath, nil
	}
	if strings.HasPrefix(rel, "/") {
		return "", errors.New("只能使用Web根目录内的相对路径")
	}
	for _, segment := range strings.Split(rel, "/") {
		if segment == ".." {
			return "", errors.New("路径不能包含上级目录")
		}
	}
	cleanRel := path.Clean("/" + rel)
	if cleanRel == "/" {
		return s.rootPath, nil
	}
	if strings.HasPrefix(cleanRel, "/../") || cleanRel == "/.." {
		return "", errors.New("路径不能包含上级目录")
	}
	target := path.Clean(path.Join(s.rootPath, strings.TrimPrefix(cleanRel, "/")))
	root := path.Clean(s.rootPath)
	if root != "/" && target != root && !strings.HasPrefix(target, root+"/") {
		return "", errors.New("路径超出Web根目录")
	}
	return target, nil
}

func (s *webServer) relativeDisplayPath(target string) string {
	root := path.Clean(s.rootPath)
	target = path.Clean(target)
	if target == root {
		return "/"
	}
	if root == "/" {
		return target
	}
	return "/" + strings.TrimPrefix(strings.TrimPrefix(target, root), "/")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeWebSocketText(rw *bufio.ReadWriter, payload []byte) error {
	if len(payload) > 125 {
		if len(payload) <= 65535 {
			if _, err := rw.Write([]byte{0x81, 126}); err != nil {
				return err
			}
			var size [2]byte
			binary.BigEndian.PutUint16(size[:], uint16(len(payload)))
			if _, err := rw.Write(size[:]); err != nil {
				return err
			}
		} else {
			if _, err := rw.Write([]byte{0x81, 127}); err != nil {
				return err
			}
			var size [8]byte
			binary.BigEndian.PutUint64(size[:], uint64(len(payload)))
			if _, err := rw.Write(size[:]); err != nil {
				return err
			}
		}
	} else {
		if _, err := rw.Write([]byte{0x81, byte(len(payload))}); err != nil {
			return err
		}
	}
	if _, err := rw.Write(payload); err != nil {
		return err
	}
	return rw.Flush()
}

var webIndexTemplate = template.Must(template.New("index").Parse(`<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>云盘上传</title>
<style>
:root{--bg:#f7f8fa;--panel:#fff;--text:#15171a;--muted:#737985;--line:#e6e8ec;--primary:#1769e0;--primary-hover:#0f56bd;--green:#0f8f6f;--red:#c9342f;--amber:#9a6a12}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,"Noto Sans SC",sans-serif}button{height:36px;border:1px solid transparent;border-radius:6px;background:var(--primary);color:#fff;padding:0 14px;cursor:pointer;font-size:14px;font-weight:500}button:hover{background:var(--primary-hover)}button:disabled{cursor:not-allowed;opacity:.62}button.secondary{background:#fff;color:#252a31;border-color:var(--line)}button.secondary:hover{background:#f2f4f7}button.danger{background:#fff;color:var(--red);border-color:#f1c6c3}button.danger:hover{background:#fff5f4}input[type=file]{display:none}.top{background:#fff;border-bottom:1px solid var(--line)}.top-inner{max-width:1240px;margin:0 auto;padding:18px 24px}.title{display:flex;justify-content:space-between;gap:16px;align-items:center}.title h1{font-size:20px;line-height:1.3;margin:0 0 4px;font-weight:650}.title p{margin:0;color:var(--muted);font-size:13px}.shell{max-width:1240px;margin:0 auto;padding:20px 24px}.grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(320px,380px);gap:18px;align-items:start}.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;overflow:hidden}.panel-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;padding:14px 16px;border-bottom:1px solid var(--line)}.panel-head h2{font-size:15px;margin:0;font-weight:650}.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.crumbs{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-top:8px}.crumb{border-radius:6px;background:#f2f5f9;color:#2b3440;padding:5px 8px;cursor:pointer;font-size:13px}.crumb:hover{background:#e8edf4}.drop{margin:16px;border:1px dashed #aeb8c6;border-radius:8px;padding:28px 18px;text-align:center;background:#fbfcfe;transition:border-color .15s,background .15s}.drop.drag{border-color:var(--primary);background:#f0f6ff}.drop strong{display:block;font-size:17px;margin-bottom:7px;font-weight:650}.drop .muted{margin-bottom:16px}.file-row,.task-row{display:grid;gap:12px;align-items:center;padding:12px 16px;border-top:1px solid #f0f2f5}.file-row{grid-template-columns:minmax(0,1fr) 112px 160px}.task-row{grid-template-columns:minmax(0,1fr) 82px}.file-row:hover{background:#fbfcfe}.file-row:first-child,.task-row:first-child{border-top:0}.file-main{display:flex;align-items:center;gap:11px;min-width:0}.material-icon{width:28px;height:28px;flex:0 0 28px}.material-icon img{display:block;width:28px;height:28px}.name{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.folder{color:#1f5faa;cursor:pointer;font-weight:600}.file-icon{color:#343a42}.muted{color:var(--muted);font-size:13px}.empty{padding:30px 16px;text-align:center;color:var(--muted);font-size:14px}.progress{height:6px;background:#edf0f4;border-radius:99px;overflow:hidden;margin-top:10px}.progress span{display:block;height:100%;background:var(--green);width:0;transition:width .2s}.progress.receive span{background:var(--primary)}.progress.cloud span{background:var(--green)}.dual-progress{display:grid;gap:8px;margin-top:10px}.progress-line{display:grid;grid-template-columns:62px minmax(0,1fr) 42px;gap:8px;align-items:center}.progress-line .label,.progress-line .percent{font-size:12px;color:var(--muted)}.task-meta{display:flex;justify-content:space-between;gap:8px;margin-top:6px}.status{font-size:12px;font-weight:500;border-radius:999px;padding:4px 8px;background:#f1f3f6;color:#59616d}.status.uploading,.status.receiving{background:#eaf3ff;color:#1769e0}.status.success{background:#e7f6ef;color:var(--green)}.status.failed{background:#fff0ef;color:var(--red)}.status.queued{background:#fff6df;color:var(--amber)}.ws{display:flex;align-items:center;gap:7px;color:var(--muted);font-size:13px}.dot{width:7px;height:7px;border-radius:50%;background:#c9342f}.dot.on{background:#0f8f6f}.toast{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);background:#15171a;color:#fff;padding:10px 14px;border-radius:7px;display:none;max-width:min(520px,calc(100vw - 32px));font-size:14px}
@media(max-width:1020px){.grid{grid-template-columns:1fr}.shell{padding:16px}.top-inner{padding:16px}.drop{margin:12px}.file-row{grid-template-columns:minmax(0,1fr) 100px 140px}}@media(max-width:680px){.title{align-items:flex-start;flex-direction:column}.panel-head{flex-direction:column}.toolbar{width:100%}.toolbar button{flex:1}.file-row{grid-template-columns:1fr;gap:7px;padding:14px}.file-row .muted{padding-left:39px}.task-row{grid-template-columns:1fr}.task-meta{display:block}.hide-md{display:none}.material-icon{width:28px;height:28px}.drop{padding:24px 12px}}
</style>
</head>
<body>
<header class="top"><div class="top-inner"><div class="title"><div><h1>云盘上传</h1><p>当前目录范围：{{.RootPath}}</p></div><div class="ws"><span id="wsDot" class="dot"></span><span id="wsText">连接中</span></div></div></div></header>
<main class="shell">
  <div class="grid">
    <section class="panel">
      <div class="panel-head"><div><h2>文件</h2><div class="crumbs" id="crumbs"></div></div><div class="toolbar"><button class="secondary" onclick="goUp()">上级</button><button id="pickBtn" onclick="pickFiles()">选择文件</button><input id="files" type="file" multiple onchange="uploadFiles(this.files)"></div></div>
      <div id="drop" class="drop"><strong>拖拽文件到这里上传</strong><div class="muted">支持多文件、8MB分片、失败重试和断点续传</div><button id="dropPickBtn" onclick="pickFiles()">选择文件</button></div>
      <div id="filesPanel"></div>
    </section>
    <aside class="panel">
      <div class="panel-head"><h2>上传任务</h2><button class="danger" onclick="clearHistory()">清空历史</button></div>
      <div id="tasksPanel"></div>
    </aside>
  </div>
</main>
<div id="toast" class="toast"></div>
<script>
let currentPath = '/';
let ws;
let lastPickAt = 0;
const submittingFingerprints = new Set();
const filesInput = document.getElementById('files');
const drop = document.getElementById('drop');
const pickBtn = document.getElementById('pickBtn');
const dropPickBtn = document.getElementById('dropPickBtn');
function enc(v){ return encodeURIComponent(v === '/' ? '' : v.replace(/^\//,'')); }
function relPath(){ return currentPath === '/' ? '' : currentPath.replace(/^\//,''); }
function fmt(bytes){ if(!bytes) return '0 B'; const u=['B','KB','MB','GB','TB']; let i=0,n=bytes; while(n>=1024&&i<u.length-1){n/=1024;i++} return n.toFixed(n>=10||i===0?0:1)+' '+u[i]; }
function esc(s){return String(s||'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function statusText(status){return ({receiving:'接收中',queued:'等待上传',uploading:'上传中',success:'已完成',failed:'失败'})[status]||status}
function extOf(name){const i=String(name||'').lastIndexOf('.');return i>0?name.slice(i+1).toLowerCase():''}
function materialIcon(file){
  const icon = iconName(file);
  return '<span class="material-icon"><img src="/assets/web/material-icons/'+icon+'.svg" alt="" loading="lazy" onerror="this.src=\'/assets/web/material-icons/document.svg\'"></span>';
}
function iconName(file){
  if(file.isFolder) return 'folder-base';
  const ext=extOf(file.fileName);
  if(['js','mjs','cjs','jsx'].includes(ext)) return 'javascript';
  if(['ts','tsx'].includes(ext)) return 'typescript';
  if(ext==='go') return 'go';
  if(ext==='py') return 'python';
  if(ext==='rs') return 'rust';
  if(['java','kt'].includes(ext)) return 'java';
  if(ext==='html') return 'html';
  if(['css','scss','sass','less'].includes(ext)) return 'css';
  if(['json','yaml','yml','toml','xml'].includes(ext)) return 'json';
  if(['md','markdown'].includes(ext)) return 'markdown';
  if(ext==='pdf') return 'pdf';
  if(['jpg','jpeg','png','gif','webp','svg','heic','bmp','ico'].includes(ext)) return 'image';
  if(['mp4','mkv','mov','avi','wmv','flv','webm'].includes(ext)) return 'video';
  if(['mp3','flac','wav','aac','ogg'].includes(ext)) return 'audio';
  if(['zip','rar','7z','tar','gz','bz2','xz'].includes(ext)) return 'zip';
  if(ext==='vue') return 'vue';
  return 'document';
}
function toast(msg){const el=document.getElementById('toast');el.textContent=msg;el.style.display='block';clearTimeout(window.toastTimer);window.toastTimer=setTimeout(()=>el.style.display='none',2600)}
function setWs(on){document.getElementById('wsDot').classList.toggle('on',on);document.getElementById('wsText').textContent=on?'实时连接':'连接断开'}
async function loadFiles(){
  const res = await fetch('/api/files?path='+enc(currentPath));
  const data = await res.json();
  if(!res.ok){ toast(data.error || '加载失败'); return; }
  currentPath = data.path || '/'; renderCrumbs();
  const rows = (data.files||[]).map(f=>{
    const name=esc(f.fileName);
    const title=f.isFolder?'<span class="folder" onclick="enterFolder(\''+encodeURIComponent(f.fileName)+'\')">'+name+'</span>':'<span class="file-icon">'+name+'</span>';
    return '<div class="file-row"><div class="file-main">'+materialIcon(f)+'<div class="name">'+title+'</div></div><div class="muted">'+(f.isFolder?'文件夹':fmt(f.fileSize))+'</div><div class="muted hide-md">'+esc(f.updatedAt||'')+'</div></div>';
  }).join('') || '<div class="empty">当前目录为空</div>';
  document.getElementById('filesPanel').innerHTML=rows;
}
function renderCrumbs(){
  const parts=currentPath==='/'?[]:currentPath.replace(/^\//,'').split('/');
  let html='<span class="crumb" onclick="jumpPath(0)">/</span>';
  parts.forEach((p,i)=>{html+='<span class="muted">/</span><span class="crumb" onclick="jumpPath('+(i+1)+')">'+esc(p)+'</span>'});
  document.getElementById('crumbs').innerHTML=html;
}
function jumpPath(depth){ if(depth===0) currentPath='/'; else currentPath='/'+currentPath.replace(/^\//,'').split('/').slice(0,depth).join('/'); loadFiles(); }
function enterFolder(name){ const n=decodeURIComponent(name); currentPath=(currentPath==='/'?'/':currentPath+'/')+n; loadFiles(); }
function goUp(){ if(currentPath==='/') return; currentPath=currentPath.replace(/\/+$/,'').split('/').slice(0,-1).join('/')||'/'; loadFiles(); }
function setPickBusy(busy){ pickBtn.disabled=busy; dropPickBtn.disabled=busy; pickBtn.textContent=busy?'处理中':'选择文件'; dropPickBtn.textContent=busy?'处理中':'选择文件'; }
function pickFiles(){
  const now=Date.now();
  if(now-lastPickAt<700) return;
  lastPickAt=now;
  filesInput.click();
}
const CHUNK_SIZE = 8 * 1024 * 1024;
const CHUNK_CONCURRENCY = 3;
const CHUNK_RETRY = 3;
function fingerprint(file){ return [file.name,file.size,file.lastModified,currentPath].join('|'); }
async function uploadFiles(list){
  const files=Array.from(list||[]); if(!files.length) return;
  filesInput.value='';
  const pending=files.filter(file=>!submittingFingerprints.has(fingerprint(file)));
  if(!pending.length){ toast('这些文件已在提交中'); return; }
  toast('开始分片上传 '+pending.length+' 个文件');
  setPickBusy(true);
  for(const file of pending){
    const fp=fingerprint(file);
    submittingFingerprints.add(fp);
    uploadChunkedFile(file).catch(err=>toast(file.name+' 上传失败：'+err.message)).finally(()=>{submittingFingerprints.delete(fp); if(!submittingFingerprints.size) setPickBusy(false);});
  }
}
async function uploadChunkedFile(file){
  const total=Math.max(1, Math.ceil(file.size / CHUNK_SIZE));
  const initRes=await fetch('/api/upload/init',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({fileName:file.name,size:file.size,chunkSize:CHUNK_SIZE,chunkTotal:total,fingerprint:fingerprint(file),path:relPath()})});
  const init=await initRes.json(); if(!initRes.ok) throw new Error(init.error||'初始化失败');
  localStorage.setItem('aliyunpan-upload-'+fingerprint(file), init.uploadId);
  const done=new Set(init.received||[]);
  let next=0, active=0, failed=false;
  await new Promise((resolve,reject)=>{
    const pump=()=>{
      if(failed) return;
      while(active<CHUNK_CONCURRENCY && next<total){
        const idx=next++; if(done.has(idx)){ continue; }
        active++;
        uploadOneChunk(init.uploadId,file,idx,total).then(()=>{done.add(idx);active--;pump();}).catch(err=>{failed=true;reject(err);});
      }
      if(!failed && next>=total && active===0) resolve();
    };
    pump();
  });
  const completeRes=await fetch('/api/upload/complete?uploadId='+encodeURIComponent(init.uploadId),{method:'POST'});
  const complete=await completeRes.json(); if(!completeRes.ok) throw new Error(complete.error||'合并失败');
  localStorage.removeItem('aliyunpan-upload-'+fingerprint(file)); toast(file.name+' 已进入云盘上传队列');
}
async function uploadOneChunk(uploadId,file,idx,total){
  const start=idx*CHUNK_SIZE, end=Math.min(file.size,start+CHUNK_SIZE);
  const blob=file.slice(start,end);
  let lastErr;
  for(let attempt=0; attempt<CHUNK_RETRY; attempt++){
    try{
      const res=await fetch('/api/upload/chunk?uploadId='+encodeURIComponent(uploadId)+'&index='+idx,{method:'PUT',body:blob});
      if(res.ok) return;
      const data=await res.json().catch(()=>({})); lastErr=new Error(data.error||('分片 '+(idx+1)+'/'+total+' 上传失败'));
    }catch(err){ lastErr=err; }
    await new Promise(r=>setTimeout(r, 500*(attempt+1)));
  }
  throw lastErr || new Error('分片上传失败');
}
function renderTasks(tasks){
  const rows=(tasks||[]).map(t=>{
    const receiveDone=t.status==='success'?t.size:t.received;
    const cloudDone=t.status==='success'?t.size:t.uploaded;
    const receivePct=t.size?Math.min(100,Math.round(receiveDone/t.size*100)):0;
    const cloudPct=t.size?Math.min(100,Math.round(cloudDone/t.size*100)):0;
    const err=t.error?'<div class="muted">'+esc(t.error)+'</div>':'';
    return '<div class="task-row"><div><div class="name"><strong>'+esc(t.fileName)+'</strong></div><div class="task-meta"><span class="muted">'+fmt(receiveDone)+' / '+fmt(t.size)+' · '+fmt(t.speed)+'/s</span>'+err+'</div><div class="dual-progress"><div class="progress-line"><span class="label">本机接收</span><div class="progress receive"><span style="width:'+receivePct+'%"></span></div><span class="percent">'+receivePct+'%</span></div><div class="progress-line"><span class="label">云盘上传</span><div class="progress cloud"><span style="width:'+cloudPct+'%"></span></div><span class="percent">'+cloudPct+'%</span></div></div></div><div><span class="status '+esc(t.status)+'">'+statusText(t.status)+'</span></div></div>';
  }).join('') || '<div class="empty">暂无上传任务</div>';
  document.getElementById('tasksPanel').innerHTML=rows;
}
async function loadTasksFallback(){ const res=await fetch('/api/uploads'); const data=await res.json(); if(res.ok) renderTasks(data.tasks||[]); }
function connectWs(){
  const proto=location.protocol==='https:'?'wss':'ws'; ws=new WebSocket(proto+'://'+location.host+'/api/ws');
  ws.onopen=()=>setWs(true); ws.onclose=()=>{setWs(false); setTimeout(connectWs,1200)}; ws.onerror=()=>ws.close();
  ws.onmessage=e=>{try{const data=JSON.parse(e.data); if(data.type==='uploads') renderTasks(data.tasks||[])}catch(_){}};
}
async function clearHistory(){
  const res=await fetch('/api/uploads/history',{method:'DELETE'}); const data=await res.json();
  if(!res.ok){toast(data.error||'清空失败');return}
  renderTasks(data.tasks||[]); toast('已清空 '+(data.removed||0)+' 条历史');
}
['dragenter','dragover'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.add('drag')}));
['dragleave','drop'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.remove('drag')}));
drop.addEventListener('drop',e=>uploadFiles(e.dataTransfer.files));
loadFiles(); loadTasksFallback(); connectWs();
</script>
</body>
</html>`))
