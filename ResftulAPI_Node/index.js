const express = require('express')
const Validator = require('jsonschema').Validator;
const redis = require('redis');
const bodyParser = require("body-parser");
const bcrypt = require("bcrypt");
const schema = require("./schema.js");
const jwt = require('jsonwebtoken');
const etag = require('etag');
const kafka = require('kafka-node');
const KeyedMessage = kafka.KeyedMessage;
const config = require('./config');
const PORT = process.env.PORT || 3000;

const app = express();
const v  = new Validator();
const client = redis.createClient("6379", "127.0.0.1");

const postMessage = async (message) => {
    const Producer = kafka.Producer;
    const client = new kafka.KafkaClient(config.kafka_server);
    const producer = new Producer(client);
    const kafka_topic = config.kafka_topic;
    console.log("kafka_topic: " + kafka_topic);
    console.log("Message: " + message);
    let payloads = [
        {
            topic: kafka_topic,
            key: "index",
            messages: JSON.stringify(message)
        }
    ];

    producer.on('ready', async function() {
        producer.send(payloads, (err, data) => {
            if (err) {
                console.log('[kafka-producer -> ' + kafka_topic + ']: broker update failed' + "  due to :" + err);
            } else {
                console.log('[kafka-producer -> ' + kafka_topic + ']: broker update success');
            }
        });
    });

    producer.on('error', function(err) {
        console.log(err);
        console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
        throw err;
    });

}

const deleteMessage = async (id) => {
    const Producer = kafka.Producer;
    const client = new kafka.KafkaClient(config.kafka_server);
    const producer = new Producer(client);
    const kafka_topic = config.kafka_topic;
    console.log("kafka_topic: " + kafka_topic);
    console.log("ID: " + id);
    let payloads = [
        {
            topic: kafka_topic,
            key: "delete",
            messages: id
        }
    ];

    producer.on('ready', async function() {
        producer.send(payloads, (err, data) => {
            if (err) {
                console.log('[kafka-producer -> ' + kafka_topic + ']: broker update failed' + " due to : " + err);
            } else {
                console.log('[kafka-producer -> ' + kafka_topic + ']: broker update success');
            }
        });
    });

    producer.on('error', function(err) {
        console.log(err);
        console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
        throw err;
    });

}

const createObj = async (body, client, req, res) => {
    const ids = [];
    const newObj = Object.assign({}, body);
    newObj["planCostShares"] = newObj["planCostShares"].objectId;
    newObj["linkedPlanServices"] = [newObj["linkedPlanServices"][0].objectId, newObj["linkedPlanServices"][1].objectId];
    ids.push(newObj.objectId);
    await client.set(newObj.objectId, JSON.stringify(newObj), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }

        console.log(newObj.objectId);
        
    });

    ids.push(body["planCostShares"].objectId);
    await client.set(body["planCostShares"].objectId, JSON.stringify(body["planCostShares"]), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }
        console.log(body["planCostShares"].objectId);
        
    });

    let newObj1 = Object.assign({}, body["linkedPlanServices"][0]);
    newObj1["linkedService"] = newObj1["linkedService"].objectId;
    newObj1["planserviceCostShares"] = newObj1["planserviceCostShares"].objectId;
    ids.push(newObj1.objectId);
    await client.set(newObj1.objectId, JSON.stringify(newObj1), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }
        console.log(newObj1.objectId);
    });

    let newObj2 = Object.assign({}, body["linkedPlanServices"][1]);
    newObj2["linkedService"] = newObj2["linkedService"].objectId;
    newObj2["planserviceCostShares"] = newObj2["planserviceCostShares"].objectId;
    ids.push(newObj2.objectId);
    await client.set(newObj2.objectId, JSON.stringify(newObj2), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }
        console.log(newObj2.objectId);
    });

    newObj1 = Object.assign({}, body["linkedPlanServices"][0]);
    ids.push(newObj1["linkedService"].objectId);
    await client.set(newObj1["linkedService"].objectId, JSON.stringify(newObj1["linkedService"]), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }
        console.log(newObj1["linkedService"].objectId);
    });

    ids.push(newObj1["planserviceCostShares"].objectId);
    await client.set(newObj1["planserviceCostShares"].objectId, JSON.stringify(newObj1["planserviceCostShares"]), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }
        console.log(newObj1["planserviceCostShares"].objectId);
    });

    newObj2 = Object.assign({}, body["linkedPlanServices"][1]);
    ids.push(newObj2["linkedService"].objectId);
    await client.set(newObj2["linkedService"].objectId, JSON.stringify(newObj2["linkedService"]), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }
        console.log(newObj2["linkedService"].objectId);
    });

    ids.push(newObj2["planserviceCostShares"].objectId);
    await client.set(newObj2["planserviceCostShares"].objectId, JSON.stringify(newObj2["planserviceCostShares"]), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }

        console.log(newObj2["planserviceCostShares"].objectId);
    });

    await client.set(req.user, JSON.stringify(ids), async (err) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 400});
            return;
        }
    });
}

const deleteObject = async (index, ids, body, client, req, res, isDelete) => {
    if (index == ids.length) {
        if (isDelete) {
            res.send("Successfully delete the element");
        } else {
            await createObj(body, client, req, res);
            res.send("Successfully update the element");
        }
        return;
    }

    await client.del(ids[index], async (err, reply) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 500});
            return;
        }

        await deleteObject(index + 1, ids, body, client, req, res, isDelete);
    });
}

app.enable('etag');
app.use( bodyParser.json({ extended: true, type: '*/*' }) );
app.use((req, res, next) => {
    if (req.headers && req.headers.authorization) {
        jwt.verify(req.headers.authorization.split(" ")[1], 'RESTFULAPIS', (err, decode) => {
            if (err) {
                res.status(401).json({success: false, error: "Unauthorized user", statusCode: 401});
                return;
            }

            console.log(decode);
            req.user = decode.username;
            next();
        });
    } else {
        req.user = undefined;
        next();
    }
});

v.addSchema(schema.costSharesSchema, '/CostShares');
v.addSchema(schema.linkedServiceSchema, '/LinkedService');
v.addSchema(schema.linkedPlanServiceSchema, '/LinkedService');

app.post("/register", (req, res) => {

    const username = req.body.username;
    const password = req.body.password;
    client.select(1, () => {
        client.set(username, bcrypt.hashSync(password, 10), (err, reply) => {
            if (err) {
                console.log(err);
                res.status(500).json({success: false, error: err, statusCode: 400});
                return;
            }

            res.send(JSON.stringify(reply));
        });
    });
});

app.post("/signIn", (req, res) => {
    const username = req.body.username;
    const password = req.body.password;

    client.select(1, () => {
        client.get(username, (err, reply) => {
            if (err) {
                console.log(err);
                res.status(500).json({success: false, error: err, statusCode: 400});
                return;
            }
            
            if (reply == null) {
                res.status(401).json({success: false, error: "User doesn't exist in the database", statusCode: 401})
            } else {
                if (bcrypt.compareSync(password, reply)) {
                    const expiration = Math.floor(Date.now() / 1000) + (60 * 60)
                    res.json({token: jwt.sign({username: username, exp: expiration}, 'RESTFULAPIS'), message: "Sign In successfully", expiration: new Date(Date.now() + (60 * 60)).toUTCString()});
                } else {
                    res.status(401).json({success: false, error: "Username/Password is wrong", statusCode: 401})
                }
            }
        });
    });
});

app.get("/getPlan", async (req, res) => {
    if (!req.user) {
        res.status(401).json({success: false, error: "Unauthorized user", statusCode: 401});
        return;
    }

    await client.select(0, async () => {
        let ids = [];
        res.setHeader('ETag', etag(JSON.stringify(req.user)));
        client.get(req.user, async (err, reply) => {
        if (err) {
            console.log(err);
            res.status(500).json({success: false, error: err, statusCode: 500});
            return;
        }
        
            res.send(reply);
        })
        
    });    
})

app.post("/createPlan", async (req, res) => {

    if (!req.user) {
        res.status(401).json({success: false, error: "Unauthorized user", statusCode: 401});
        return;
    }

    res.setHeader('ETag', etag(JSON.stringify(req.body)));
    if (req.headers["if-none-match"] == etag(JSON.stringify(req.body))) {
        res.status(304).json("Body Not Modified");
        return;
    }

    const body = req.body;

    if (!v.validate(body, schema.schema).valid) {
        res.status(400).json({success: false, error: "Validation Error", statusCode: 400});
        return;
    }

    await client.select(0, async () => {
        await client.get(req.user, async (err, reply) => {
            if (err) {
                console.log(err);
                res.status(500).json({success: false, error: err, statusCode: 400});
                return;
            }
        
            if (reply != null) {
                res.send(JSON.stringify({success: false, error: "The objectId already exist in database"}));
            } else {
                await client.set(req.user, JSON.stringify(req.body), async (err, reply) => {
                    if (err) {
                        console.log(err);
                        res.status(500).json({success: false, error: err, statusCode: 400});
                        return;
                    }

                    await postMessage(req.body);
                    res.send(reply);
                });
                
            }
        });
    });

})



app.put("/updatePlan", async (req, res) => {
    const objectId = req.query.objectId;

    if (!req.user) {
        res.status(401).json({success: false, error: "Unauthorized user", statusCode: 401});
        return;
    }

    res.setHeader('ETag', etag(JSON.stringify(req.body)));
    if (req.headers["if-none-match"] == etag(JSON.stringify(req.body))) {
        res.status(304).json("Body Not Modified");
        return;
    }

    await client.select(0, async (err, reply) => {
        await client.set(req.user, JSON.stringify(req.body), (err, reply) => {
            if (err) {
                console.log(err);
                res.status(500).json({success: false, error: err, statusCode: 400});
                return;
            }

            if (reply == null) {
                res.send(JSON.stringify({success: false, error: "The objectId doesn't exist in database!"}));
                return;
            }

            res.send("Successfully update the element");
        });
    });
});

app.patch("/updatePartPlan", async (req, res) => {
    if (!req.user) {
        res.status(401).json({success: false, error: "Unauthorized user", statusCode: 401});
        return;
    }

    res.setHeader('ETag', etag(JSON.stringify(req.body)));
    if (req.headers["if-none-match"] == etag(JSON.stringify(req.body))) {
        res.status(304).json("Body Not Modified");
        return;
    }

    const obj = req.body["linkedPlanServices"];
    //const json = req.body["linkedPlanServices"][0];
    //req.body["linkedPlanServices"].push(json);

    await client.select(0, async () => {
        await client.get(req.user, async (err, reply) => {
            if (reply == null) {
                res.send(JSON.stringify({success: false, error: "The objectId doesn't exist in database!"}));
                return;
            }

            const newObj = JSON.parse(reply);
            const newReply = JSON.parse(reply);
            let isAdd = true;
            const set = new Set();
            for (let i = 0; i < obj.length; i++) {
                for (let ele of newReply["linkedPlanServices"]) {
                const objectId = ele["objectId"];
                
                if (objectId == obj[i]["objectId"]) {
                    //console.log(objectId + "========" + obj[i]["objectId"]);
                    isAdd = false;
                    break;
                }
                }

                if (isAdd) {
                set.add(i);
                }
                
                isAdd = true;
            }
        
        
            //console.log("是否改变", isAdd);
            for (let id of set) {
                newObj["linkedPlanServices"].push(obj[id]);
            }
        

            await client.set(req.user, JSON.stringify(newObj), (err, reply) => {
                if (err) {
                console.log(err);
                res.status(500).json({success: false, error: err, statusCode: 400});
                return;
                }

                deleteMessage(req.body.objectId);
                postMessage(req.body);
                res.send("Successfully update the element");
            });
        });
        
    }); 
});

app.delete("/deletePlan", async (req, res) => {
    if (!req.user) {
        res.status(401).json({success: false, error: "Unauthorized user", statusCode: 401});
        return;
    }

    await client.select(0, async () => {
        await client.get(req.user, async (err, reply) => {
            if (err) {
                console.log(err);
                res.status(500).json({success: false, error: err, statusCode: 400});
                return;
            }
        
            if (reply == null) {
                res.send(JSON.stringify({success: false, error: "The objectId doesn't exist in database"}));
            } else {
                const ids = JSON.parse(reply);
                console.log(ids);
                await client.del(req.user, async (err, reply) => {
                    res.send("Succesfully delete the object");
                    await deleteMessage(req.query.id);
                    
                }); 
            }
        });
    });

})

app.listen(PORT, () => {
    console.log("App listening at :" + PORT);
});