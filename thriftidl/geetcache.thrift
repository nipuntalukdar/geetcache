namespace go thrift

enum Status {
    SUCCESS,
    FAILURE,
    KEY_EXISTS,
    KEY_NOT_EXISTS,
    EMPTY_LIST,
    LEADER_NOT_FOUND,
    NOT_LEADER,
    BAD_COMMAND
}

struct PutCommand {
    1: string Key,
    2: binary Data,
    3: i64 Expiry
}

struct DelResponse {
    1: string Key,
    2: Status Stat
}

struct LeaderResponse{
    1: Status Stat,
    2: string Leader
}

struct PeersResponse{
    1: Status Stat,
    2: optional list<string> Peers
}

struct GetResponse {
    1: string Key
    2: Status Status
    3: optional binary Data
}

struct  ListPutCommand {
    1: string ListKey
    2: bool Append = true
    3: list<binary> Values
    4: optional i64 Expiry
}

struct ListGPCommand {
    1: string ListKey
    2: i32 MaxCount = 1
    3: bool Front = true
}

struct ListGPResponse {
    1: string ListKey
    2: Status Stat
    3: i32 Retlen
    4: optional list<binary> Values
}

struct ListLenResponse {
    1: string ListKey
    2: i32 LLen
    3: Status Stat
}

struct CStatus {
    1: string Name
    2: Status Stat
    3: i64 Value
}

struct CAddCommand {
    1: string Name
    2: i64 InitialValue
    3: bool Replace
}

struct CChangeCommand {
    1: string Name
    2: i64 Delta
    3: bool ReturnOld
}

struct CASCommand {
    1: string Name
    2: i64 Expected
    3: i64 UpdVal
}

struct HLogCreateCmd {
    1: string Name
    2: i64 Expiry
}

struct HLogAddCmd {
    1: string Key
    2: binary Data 
}

struct HLogStatus {
    1: string Key
    2: optional i64 Expiry
    3: optional i64 Value
    4: Status Stat
}

service GeetcacheService {
    Status Put(1:PutCommand put)
    Status ListPut(1:ListPutCommand listPut)
    ListGPResponse ListPop(1:ListGPCommand lPop)
    ListGPResponse ListGet(1:ListGPCommand lGet)
    ListLenResponse ListLen(1:string key)
    GetResponse Get(1:string key)
    DelResponse Delete(1:string key)
    DelResponse DeleteList(1:string key)
    LeaderResponse  Leader()
    PeersResponse   Peers()
    Status AddCounter(1:CAddCommand counter)
    Status DeleteCounter(1:string counterName)
    CStatus Increament(1:CChangeCommand counter)
    CStatus Decrement(1:CChangeCommand counter)
    CStatus GetCounterValue(1:string counterName)
    Status CompSwap(1:CASCommand cas)
    HLogStatus HLogCreate(1:HLogCreateCmd hlcmd)
    HLogStatus HLogDelete(1:string key)
    HLogStatus HLogCardinality(1:string key)
    HLogStatus HLogAdd(1:HLogAddCmd hladd)
}
