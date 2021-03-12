namespace java com.song.cn.thrift

# 将数据类型重新定义成Java包含的数据类型
typedef i16 short
typedef i32 int
typedef i64 long
typedef bool boolean
typedef string String

struct Person {
    1: optional String username,
    2: optional int age,
    3: optional boolean married
}

exception DataException {
    1: optional String message,
    2: optional String callStack,
    3: optional String date
}

service PersonService {
    Person getPersonByUsername(1:required String username) throws (1: DataException dataException)
    void savePerson(1:required Person person) throws (1: DataException dataException)
}