const { MongoClient } = require('mongodb');
// 在函数开头添加
console.time('Function execution');
class MongoCache {
    constructor() {
        this.client = new MongoClient(process.env.MONGODB_URI || "mongodb+srv://gmslymhn:dNMKZeFiAXn3P856@gm.oxqdnlc.mongodb.net/?retryWrites=true&w=majority&appName=gm", {
            serverApi: { version: '1' },
            maxPoolSize: 50,  // 适当增大连接池
            minPoolSize: 5,
            connectTimeoutMS: 5000,  // 5秒连接超时
            socketTimeoutMS: 30000,  // 30秒操作超时
            waitQueueTimeoutMS: 5000, // 5秒等待连接超时
            retryWrites: true,
            retryReads: true
        });

        this.dbName = 'lz';
        this.collectionName = 'url_cache';
        this.cacheTTL = 600;
        this.connectionPromise = null;
        this.isInitialized = false;
    }

    async initialize() {
        if (!this.isInitialized) {
            await this.client.connect();

            // 创建TTL索引（只需执行一次）
            const collection = this.client.db(this.dbName).collection(this.collectionName);
            await collection.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });

            this.isInitialized = true;
            console.log('MongoDB connected and indexes verified');
        }
    }

    async getCollection() {
        if (!this.isInitialized) {
            await this.initialize();
        }
        return this.client.db(this.dbName).collection(this.collectionName);
    }

    async get(fid) {
        try {
            const collection = await this.getCollection();
            const doc = await collection.findOne({
                _id: fid,
                expiresAt: { $gt: new Date() }
            }, { maxTimeMS: 5000 });  // 5秒查询超时

            return doc?.url || null;
        } catch (err) {
            console.error('MongoCache.get error:', err);
            return null;  // 失败时返回null而不是阻断流程
        }
    }

    async set(fid, url) {
        try {
            const collection = await this.getCollection();
            await collection.updateOne(
                { _id: fid },
                {
                    $set: {
                        url,
                        expiresAt: new Date(Date.now() + this.cacheTTL * 1000),
                        createdAt: new Date()
                    }
                },
                {
                    upsert: true,
                    maxTimeMS: 5000  // 5秒操作超时
                }
            );
        } catch (err) {
            console.error('MongoCache.set error:', err);
            // 可添加重试逻辑或降级处理
        }
    }
}

// 单例模式（改进版）
let instance = null;
module.exports = () => {
    if (!instance) {
        instance = new MongoCache();
        // 预热连接（可选）
        instance.initialize().catch(console.error);
    }
    return instance;
};

process.on('SIGTERM', async () => {
    await client.close();
    process.exit(0);
});

// 在函数结尾添加
console.timeEnd('Function execution');