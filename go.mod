module 6.824/m

go 1.17

replace 6.824/m/labgob => ./src/labgob

replace 6.824/m/labrpc => ./src/labrpc

replace 6.824/m/mr => ./src/mr

replace 6.824/m/raft => ./src/raft

replace 6.824/m/porcupine => ./src/porcupine

replace 6.824/m/models => ./src/models

replace 6.824/m/shardmaster => ./src/shardmaster

//replace 6.824/m/mrapps => ./src/mrapps

//replace 6.824/m/main => ./src/main

require (
	6.824/m/labgob v0.0.0-00010101000000-000000000000
	6.824/m/labrpc v0.0.0-00010101000000-000000000000
	6.824/m/models v0.0.0-00010101000000-000000000000
	6.824/m/mr v0.0.0-00010101000000-000000000000
	6.824/m/porcupine v0.0.0-00010101000000-000000000000
	6.824/m/raft v0.0.0-00010101000000-000000000000
	6.824/m/shardmaster v0.0.0-00010101000000-000000000000
)
