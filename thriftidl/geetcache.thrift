namespace go main

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


service GeetcacheService {
    Status Put(1:PutCommand put)
    Status ListPut(1:ListPutCommand listPut)
    ListGPResponse ListPop(1:ListGPCommand lPop)
    ListGPResponse ListGet(1:ListGPCommand lGet)
    GetResponse Get(1:string key)
    DelResponse Delete(1:string key)
    DelResponse DeleteList(1:string key)
    LeaderResponse  Leader()
    PeersResponse   Peers()
}
