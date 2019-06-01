# synctool

## 一个p2p的多节点的文件同步程序
最新的修改在risk分支里
```
使用golang实现的同步程序,该程序以p2p的架构运行,在集群中的节点定期交换文件元数据,
以此来保证程序运行中，每个节点都可以保证自己本地的数据都是最新的
```
*实现了[bep](https://docs.syncthing.net/specs/bep-v1.html)协议来进行同步*
  
## 程序中使用的技术:

* 基于libp2p实现节点间的通信 
* 数据的序列化和反序列化使用的是protocol buffer  
* 程序使用额外的逻辑层在监视文件时，并为文件生成index ,和一系列的indexUpdate ,来描述文件的变化.
* 使用linux提供的inotify和epoll接口实现的文件夹监视器   
* 使用golang的http包来实现与用户进行交互的功能 
* 使用sqlite数据库来完成数据的持久化 
* 在管理多个共享文件夹时，为了保持并发，使用到了分段锁机制  
 
  
## 用户接口
```
程序没有自带的ui界面，但是提供基于http的访问接口，
用户可以使用任何形式的客户端以类似访问web的形式访问程序 

Message {
    State   string     (State 的值为ok或err)  
    Text    string     (Text 用于出现错误时描述)  
}


FolderOpts {
    Id          string 
    Real        string
    ReadOnly    bool 
    Devices     []string
    Label       string 
}

```

|URL | Method | Post | Response |Description|
|:----|:--------|:-----|:----------|:--------|
|/addFolder|POST | FolderOpts | Message  | 添加新的共享文件夹 
|/addDevice|POST | DeviceId, HostId |   Message   | 添加新的设备节点
|/editFolder|POST | FolderId,FolderOpts |    Message  | 修改共享文件夹属性 
|/removeFolder|POST | DeviceId |  Message    | 移除共享文件夹 
|/removeDevice|POST | DeviceId | Message     | 移除设备 
|/devices|GET |  | devices     | 获取设备列表 
|/folders|GET |  | folders     | 获取文件夹信息
