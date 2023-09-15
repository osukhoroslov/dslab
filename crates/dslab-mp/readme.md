DSLab MP is a library for simulation and testing of distributed systems based on a message passing model. It allows to 
build, execute and test models of systems consisting of a set of processes that are executed on a set of nodes connected 
by a network and interact with each other by sending and receiving messages. The library supports simulation of common 
problems found in distributed systems, such as node crashes and network failures. For example, the network model can 
drop, duplicate, corrupt, delay or reorder messages. It is also possible to control individual network links and 
simulate network partitions. In addition to Rust, the library supports implementing a process logic in Python. 

DSLab MP is used in homework assignments for [Distributed Systems course](https://github.com/osukhoroslov/distsys-course-hse) 
at HSE University. In each assignment students should implement a system with some required properties by implementing a 
logic of system processes. The correctness and other properties of a student's solution are checked by means of 
hand-crafted and randomized tests performing simulation of system execution with DSLab MP. If the error is found, the 
trace of system execution leading to the error is output to the student. Deterministic simulation allows to quickly 
reproduce errors until they are fixed. In addition to simulation-based testing, an experimental support for more 
comprehensive testing based on model checking has been implemented recently.
