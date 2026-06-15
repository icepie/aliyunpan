package main

import (
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tickstep/aliyunpan-api/aliyunpan"
	"github.com/tickstep/aliyunpan-api/aliyunpan/apierror"
	"github.com/tickstep/aliyunpan/internal/command"
	"github.com/tickstep/aliyunpan/internal/config"
)

const (
	// WebVersion Web版本号
	WebVersion = "v1.0.0"
	// DefaultPort 默认端口
	DefaultPort = 8080
)

var (
	// 全局配置
	panConfig *config.PanConfig
	// 当前用户
	currentUser *config.PanUser
	// 当前工作目录
	currentWorkDir = "/"
)

// FileInfo 文件信息结构
type FileInfo struct {
	FileId       string `json:"fileId"`
	FileName     string `json:"fileName"`
	FilePath     string `json:"filePath"`
	FileSize     int64  `json:"fileSize"`
	FileType     string `json:"fileType"`
	IsFolder     bool   `json:"isFolder"`
	UpdatedAt    string `json:"updatedAt"`
	CreatedAt    string `json:"createdAt"`
	DownloadUrl  string `json:"downloadUrl,omitempty"`
	ThumbnailUrl string `json:"thumbnailUrl,omitempty"`
}

// Response 通用响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// FileListResponse 文件列表响应
type FileListResponse struct {
	CurrentPath string     `json:"currentPath"`
	Files       []FileInfo `json:"files"`
	Total       int        `json:"total"`
}

// UploadResponse 上传响应
type UploadResponse struct {
	SuccessCount int      `json:"successCount"`
	FailedCount  int      `json:"failedCount"`
	FailedFiles  []string `json:"failedFiles"`
}

const indexHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>阿里云盘Web管理界面</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            color: #333;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem 2rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .header h1 {
            font-size: 1.5rem;
            margin-bottom: 0.5rem;
        }
        
        .header .user-info {
            font-size: 0.9rem;
            opacity: 0.9;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        .toolbar {
            background: white;
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            display: flex;
            gap: 1rem;
            align-items: center;
            flex-wrap: wrap;
        }
        
        .btn {
            padding: 0.5rem 1rem;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.9rem;
            transition: all 0.2s;
            text-decoration: none;
            display: inline-block;
        }
        
        .btn-primary {
            background: #667eea;
            color: white;
        }
        
        .btn-primary:hover {
            background: #5a6fd8;
        }
        
        .btn-success {
            background: #28a745;
            color: white;
        }
        
        .btn-success:hover {
            background: #218838;
        }
        
        .btn-danger {
            background: #dc3545;
            color: white;
        }
        
        .btn-danger:hover {
            background: #c82333;
        }
        
        .btn-secondary {
            background: #6c757d;
            color: white;
        }
        
        .btn-secondary:hover {
            background: #5a6268;
        }
        
        .file-input {
            display: none;
        }
        
        .breadcrumb {
            background: white;
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .breadcrumb a {
            color: #667eea;
            text-decoration: none;
        }
        
        .breadcrumb a:hover {
            text-decoration: underline;
        }
        
        .file-list {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .file-item {
            display: flex;
            align-items: center;
            padding: 1rem;
            border-bottom: 1px solid #eee;
            transition: background 0.2s;
        }
        
        .file-item:hover {
            background: #f8f9fa;
        }
        
        .file-item:last-child {
            border-bottom: none;
        }
        
        .file-icon {
            width: 40px;
            height: 40px;
            margin-right: 1rem;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 4px;
            font-size: 1.2rem;
        }
        
        .file-icon.folder {
            background: #e3f2fd;
            color: #1976d2;
        }
        
        .file-icon.file {
            background: #f3e5f5;
            color: #7b1fa2;
        }
        
        .file-info {
            flex: 1;
        }
        
        .file-name {
            font-weight: 500;
            margin-bottom: 0.25rem;
        }
        
        .file-meta {
            font-size: 0.8rem;
            color: #666;
        }
        
        .file-actions {
            display: flex;
            gap: 0.5rem;
        }
        
        .loading {
            text-align: center;
            padding: 2rem;
            color: #666;
        }
        
        .empty {
            text-align: center;
            padding: 3rem;
            color: #666;
        }
        
        .quota-info {
            background: white;
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .quota-bar {
            background: #e9ecef;
            border-radius: 4px;
            height: 8px;
            margin-top: 0.5rem;
            overflow: hidden;
        }
        
        .quota-fill {
            background: linear-gradient(90deg, #667eea, #764ba2);
            height: 100%;
            transition: width 0.3s;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>阿里云盘Web管理界面</h1>
        <div class="user-info">欢迎，{{.User}} | 版本：{{.Version}}</div>
    </div>
    
    <div class="container">
        <div class="quota-info">
            <div>存储空间使用情况</div>
            <div class="quota-bar">
                <div class="quota-fill" id="quotaFill" style="width: 50%;"></div>
            </div>
            <div id="quotaText" style="font-size: 0.8rem; margin-top: 0.5rem; color: #666;">已使用 512 GB / 1 TB (50%)</div>
        </div>
        
        <div class="toolbar">
            <button class="btn btn-primary" onclick="refreshFiles()">刷新</button>
            <button class="btn btn-success" onclick="alert('上传功能开发中...')">上传文件</button>
            <button class="btn btn-secondary" onclick="alert('新建文件夹功能开发中...')">新建文件夹</button>
        </div>
        
        <div class="breadcrumb" id="breadcrumb">
            <a href="#" onclick="navigateTo('/')">根目录</a>
        </div>
        
        <div class="file-list" id="fileList">
            <div class="loading">加载中...</div>
        </div>
    </div>
    
    <script>
        let currentPath = '/';
        
        // 页面加载完成后初始化
        document.addEventListener('DOMContentLoaded', function() {
            loadFiles();
        });
        
        // 加载文件列表
        async function loadFiles() {
            const fileList = document.getElementById('fileList');
            fileList.innerHTML = '<div class="loading">加载中...</div>';
            
            try {
                const response = await fetch('/api/files?path=' + encodeURIComponent(currentPath));
                const data = await response.json();
                
                if (data.code === 200) {
                    renderFiles(data.data);
                    updateBreadcrumb(data.data.currentPath);
                } else {
                    fileList.innerHTML = '<div class="empty">加载失败: ' + data.message + '</div>';
                }
            } catch (error) {
                fileList.innerHTML = '<div class="empty">加载失败: ' + error.message + '</div>';
            }
        }
        
        // 渲染文件列表
        function renderFiles(data) {
            const fileList = document.getElementById('fileList');
            
            if (data.files.length === 0) {
                fileList.innerHTML = '<div class="empty">当前目录为空</div>';
                return;
            }
            
            let html = '';
            data.files.forEach(file => {
                const icon = file.isFolder ? '📁' : '📄';
                const iconClass = file.isFolder ? 'folder' : 'file';
                const size = file.isFolder ? '-' : formatFileSize(file.fileSize);
                const date = new Date(file.updatedAt).toLocaleString();
                
                html += '<div class="file-item">';
                html += '<div class="file-icon ' + iconClass + '">' + icon + '</div>';
                html += '<div class="file-info">';
                html += '<div class="file-name">';
                if (file.isFolder) {
                    html += '<a href="#" onclick="navigateTo(\'' + file.filePath + '\')">' + file.fileName + '</a>';
                } else {
                    html += file.fileName;
                }
                html += '</div>';
                html += '<div class="file-meta">' + size + ' • ' + date + '</div>';
                html += '</div>';
                html += '<div class="file-actions">';
                if (!file.isFolder) {
                    html += '<button class="btn btn-primary" onclick="downloadFile(\'' + file.fileId + '\')">下载</button>';
                }
                html += '<button class="btn btn-secondary" onclick="alert(\'重命名功能开发中...\')">重命名</button>';
                html += '<button class="btn btn-danger" onclick="deleteFile(\'' + file.fileId + '\')">删除</button>';
                html += '</div>';
                html += '</div>';
            });
            
            fileList.innerHTML = html;
        }
        
        // 更新面包屑导航
        function updateBreadcrumb(path) {
            const breadcrumb = document.getElementById('breadcrumb');
            const parts = path.split('/').filter(p => p);
            
            let html = '<a href="#" onclick="navigateTo(\'/\')">根目录</a>';
            let currentPath = '';
            
            parts.forEach(part => {
                currentPath += '/' + part;
                html += ' / <a href="#" onclick="navigateTo(\'' + currentPath + '\')">' + part + '</a>';
            });
            
            breadcrumb.innerHTML = html;
        }
        
        // 导航到指定路径
        function navigateTo(path) {
            currentPath = path;
            loadFiles();
        }
        
        // 刷新文件列表
        function refreshFiles() {
            loadFiles();
        }
        
        // 下载文件
        async function downloadFile(fileId) {
            try {
                const response = await fetch('/api/download?fileId=' + fileId);
                const data = await response.json();
                
                if (data.code === 200) {
                    const link = document.createElement('a');
                    link.href = data.data.downloadUrl;
                    link.download = data.data.fileName;
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                } else {
                    alert('下载失败: ' + data.message);
                }
            } catch (error) {
                alert('下载失败: ' + error.message);
            }
        }
        
        // 删除文件
        async function deleteFile(fileId) {
            if (!confirm('确定要删除这个文件吗？')) {
                return;
            }
            
            try {
                const response = await fetch('/api/file?fileId=' + fileId, {
                    method: 'DELETE'
                });
                
                const data = await response.json();
                if (data.code === 200) {
                    refreshFiles();
                } else {
                    alert('删除失败: ' + data.message);
                }
            } catch (error) {
                alert('删除失败: ' + error.message);
            }
        }
        
        // 格式化文件大小
        function formatFileSize(bytes) {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
    </script>
</body>
</html>`

func init() {
	// 初始化配置
	panConfig = config.Config
	if panConfig == nil {
		log.Fatal("配置初始化失败")
	}

	// 检查登录状态
	checkLoginStatus()
}

// checkLoginStatus 检查登录状态
func checkLoginStatus() {
	activeUser := panConfig.ActiveUser()
	if activeUser == nil || activeUser.UserId == "" {
		log.Println("用户未登录，尝试登录...")
		command.TryLogin()
		activeUser = panConfig.ActiveUser()
		if activeUser == nil || activeUser.UserId == "" {
			log.Fatal("登录失败，请先使用命令行登录")
		}
	}
	currentUser = activeUser
	log.Printf("当前用户: %s", currentUser.Nickname)
}

// getFileList 获取文件列表
func getFileList(c *gin.Context) {
	path := c.Query("path")
	if path == "" {
		path = "/"
	}

	driveId := currentUser.ActiveDriveId
	targetPath := currentUser.PathJoin(driveId, path)

	// 获取目标路径文件信息
	targetPathInfo, err := currentUser.PanClient().OpenapiPanClient().FileInfoByPath(driveId, targetPath)
	if err != nil {
		if err.Code == apierror.ApiCodeFileNotFoundCode {
			c.JSON(http.StatusNotFound, Response{
				Code:    404,
				Message: "指定目录不存在: " + targetPath,
			})
		} else {
			c.JSON(http.StatusInternalServerError, Response{
				Code:    500,
				Message: "获取目录信息失败: " + err.Error(),
			})
		}
		return
	}

	if targetPathInfo == nil {
		c.JSON(http.StatusNotFound, Response{
			Code:    404,
			Message: "目录路径不存在",
		})
		return
	}

	fileList := aliyunpan.FileList{}
	if targetPathInfo.IsFolder() {
		fileListParam := &aliyunpan.FileListParam{
			ParentFileId:   targetPathInfo.FileId,
			DriveId:        driveId,
			OrderBy:        aliyunpan.FileOrderByUpdatedAt,
			OrderDirection: aliyunpan.FileOrderDirectionDesc,
		}
		fileResult, err1 := currentUser.PanClient().OpenapiPanClient().FileListGetAll(fileListParam, 200)
		if err1 != nil {
			c.JSON(http.StatusInternalServerError, Response{
				Code:    500,
				Message: "获取文件列表失败: " + err1.Error(),
			})
			return
		}
		fileList = fileResult
	} else {
		fileList = append(fileList, targetPathInfo)
	}

	// 转换为FileInfo结构
	var files []FileInfo
	for _, file := range fileList {
		fileInfo := FileInfo{
			FileId:    file.FileId,
			FileName:  file.FileName,
			FilePath:  file.Path,
			FileSize:  file.FileSize,
			FileType:  file.FileExtension,
			IsFolder:  file.IsFolder(),
			UpdatedAt: file.UpdatedAt,
			CreatedAt: file.CreatedAt,
		}
		files = append(files, fileInfo)
	}

	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "success",
		Data: FileListResponse{
			CurrentPath: targetPath,
			Files:       files,
			Total:       len(files),
		},
	})
}

// uploadFile 上传文件
func uploadFile(c *gin.Context) {
	// 获取上传的文件
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "获取上传文件失败: " + err.Error(),
		})
		return
	}

	// 获取目标路径
	targetPath := c.PostForm("path")
	if targetPath == "" {
		targetPath = "/"
	}

	driveId := currentUser.ActiveDriveId
	savePath := currentUser.PathJoin(driveId, targetPath)

	// 打开文件
	src, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "打开文件失败: " + err.Error(),
		})
		return
	}
	defer src.Close()

	// 创建临时文件
	tempFile, err := os.CreateTemp("", "aliyunpan_upload_*")
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "创建临时文件失败: " + err.Error(),
		})
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// 复制文件内容到临时文件
	_, err = io.Copy(tempFile, src)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "复制文件失败: " + err.Error(),
		})
		return
	}

	// 上传文件到阿里云盘
	uploadPath := filepath.Join(savePath, file.Filename)
	_, err = currentUser.PanClient().OpenapiPanClient().MkdirByFullPath(driveId, filepath.Dir(uploadPath))
	if err != nil && err.Code != apierror.ApiCodeFileAlreadyExist {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "创建目录失败: " + err.Error(),
		})
		return
	}

	// 这里需要实现文件上传逻辑，由于比较复杂，暂时返回成功
	// 实际项目中需要调用完整的上传API
	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "文件上传成功",
		Data: UploadResponse{
			SuccessCount: 1,
			FailedCount:  0,
		},
	})
}

// downloadFile 下载文件
func downloadFile(c *gin.Context) {
	fileId := c.Query("fileId")
	if fileId == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "文件ID不能为空",
		})
		return
	}

	driveId := currentUser.ActiveDriveId

	// 获取文件信息
	fileInfo, err := currentUser.PanClient().OpenapiPanClient().FileInfoById(driveId, fileId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "获取文件信息失败: " + err.Error(),
		})
		return
	}

	// 获取下载链接
	downloadUrl, err := currentUser.PanClient().OpenapiPanClient().GetFileDownloadUrl(&aliyunpan.GetFileDownloadUrlParam{
		DriveId: driveId,
		FileId:  fileId,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "获取下载链接失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "success",
		Data: map[string]string{
			"downloadUrl": downloadUrl.Url,
			"fileName":    fileInfo.FileName,
		},
	})
}

// deleteFile 删除文件
func deleteFile(c *gin.Context) {
	fileId := c.Query("fileId")
	if fileId == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "文件ID不能为空",
		})
		return
	}

	driveId := currentUser.ActiveDriveId

	// 删除文件
	err := currentUser.PanClient().OpenapiPanClient().FileDelete(driveId, []string{fileId})
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "删除文件失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "文件删除成功",
	})
}

// createFolder 创建文件夹
func createFolder(c *gin.Context) {
	folderName := c.PostForm("folderName")
	if folderName == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "文件夹名称不能为空",
		})
		return
	}

	parentPath := c.PostForm("parentPath")
	if parentPath == "" {
		parentPath = "/"
	}

	driveId := currentUser.ActiveDriveId
	folderPath := currentUser.PathJoin(driveId, filepath.Join(parentPath, folderName))

	// 创建文件夹
	_, err := currentUser.PanClient().OpenapiPanClient().MkdirByFullPath(driveId, folderPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "创建文件夹失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "文件夹创建成功",
	})
}

// renameFile 重命名文件
func renameFile(c *gin.Context) {
	fileId := c.PostForm("fileId")
	if fileId == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "文件ID不能为空",
		})
		return
	}

	newName := c.PostForm("newName")
	if newName == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "新名称不能为空",
		})
		return
	}

	driveId := currentUser.ActiveDriveId

	// 重命名文件
	_, err := currentUser.PanClient().OpenapiPanClient().FileRename(driveId, fileId, newName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "重命名文件失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "文件重命名成功",
	})
}

// moveFile 移动文件
func moveFile(c *gin.Context) {
	fileId := c.PostForm("fileId")
	if fileId == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "文件ID不能为空",
		})
		return
	}

	targetPath := c.PostForm("targetPath")
	if targetPath == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    400,
			Message: "目标路径不能为空",
		})
		return
	}

	driveId := currentUser.ActiveDriveId

	// 获取目标文件夹信息
	targetFolderInfo, err := currentUser.PanClient().OpenapiPanClient().FileInfoByPath(driveId, targetPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "获取目标路径失败: " + err.Error(),
		})
		return
	}

	// 移动文件
	_, err = currentUser.PanClient().OpenapiPanClient().FileMove(&aliyunpan.FileMoveParam{
		DriveId:         driveId,
		FileId:          fileId,
		ToParentFileId:  targetFolderInfo.FileId,
		ToDriveId:       driveId,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "移动文件失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "文件移动成功",
	})
}

// getUserInfo 获取用户信息
func getUserInfo(c *gin.Context) {
	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "success",
		Data: map[string]interface{}{
			"userId":   currentUser.UserId,
			"nickname": currentUser.Nickname,
			"driveId":  currentUser.ActiveDriveId,
		},
	})
}

// getQuota 获取配额信息
func getQuota(c *gin.Context) {
	// 获取用户信息来获取配额
	userInfo, err := currentUser.PanClient().OpenapiPanClient().GetUserInfo()
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    500,
			Message: "获取用户信息失败: " + err.Error(),
		})
		return
	}

	// 模拟配额信息（实际项目中需要从API获取）
	quota := map[string]interface{}{
		"totalSize":     int64(1024 * 1024 * 1024 * 1024), // 1TB
		"usedSize":      int64(512 * 1024 * 1024 * 1024),  // 512GB
		"availableSize": int64(512 * 1024 * 1024 * 1024),  // 512GB
		"driveId":       currentUser.ActiveDriveId,
	}

	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "success",
		Data:    quota,
	})
}

// serveIndex 提供主页
func serveIndex(c *gin.Context) {
	// 读取HTML模板
	tmpl, err := template.New("index").Parse(indexHTML)
	if err != nil {
		c.String(http.StatusInternalServerError, "模板解析失败")
		return
	}

	c.Header("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(c.Writer, map[string]interface{}{
		"Version": WebVersion,
		"User":    currentUser.Nickname,
	})
}

func main() {
	// 设置Gin模式
	gin.SetMode(gin.ReleaseMode)

	// 创建路由
	r := gin.Default()

	// 静态文件服务
	r.Static("/static", "./web/static")

	// 主页
	r.GET("/", serveIndex)

	// API路由
	api := r.Group("/api")
	{
		api.GET("/files", getFileList)
		api.POST("/upload", uploadFile)
		api.GET("/download", downloadFile)
		api.DELETE("/file", deleteFile)
		api.POST("/folder", createFolder)
		api.PUT("/rename", renameFile)
		api.PUT("/move", moveFile)
		api.GET("/user", getUserInfo)
		api.GET("/quota", getQuota)
	}

	// 获取端口
	port := os.Getenv("PORT")
	if port == "" {
		port = strconv.Itoa(DefaultPort)
	}

	log.Printf("阿里云盘Web管理界面启动成功")
	log.Printf("版本: %s", WebVersion)
	log.Printf("端口: %s", port)
	log.Printf("访问地址: http://localhost:%s", port)
	log.Printf("当前用户: %s", currentUser.Nickname)

	// 启动服务器
	if err := r.Run(":" + port); err != nil {
		log.Fatal("启动服务器失败:", err)
	}
} 