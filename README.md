Code for [my post](https://tirkarthi.github.io/programming/2018/08/17/redis-streams-clojure.html) on redis streams.

### Requirements

* Redis 5.0 (RC 1) and above that has streams or use the unstable branch to compile Redis yourself.

### Installation

* Clone the repo
* To injest data `clj -m producer`
* To start a consumer `clj -m consumer <consumer_name>`

### License

Copyright © 2018 Karthikeyan S

Distributed under the MIT License
