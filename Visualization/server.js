const express = require('express');
const cors = require('cors');
const neo4j = require('neo4j-driver');

const app = express();
app.use(cors());
app.use(express.json());

// 配置 Neo4j 连接
const driver = neo4j.driver(
    'bolt://localhost:7687',
    neo4j.auth.basic('neo4j', 'lmz20040429')
);

// 通用查询接口
app.post('/query', async (req, res) => {
    const { query } = req.body;
    const session = driver.session();
    try {
        const result = await session.run(query);

        // 转换 Int64 数据为标准数字
        const data = result.records.map(record => {
            const convertedRecord = {};
            record.keys.forEach(key => {
                const value = record.get(key);
                convertedRecord[key] = neo4j.isInt(value) ? neo4j.integer.toNumber(value) : value;
            });
            return convertedRecord;
        });

        res.json({ success: true, data });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    } finally {
        await session.close();
    }
});

// 优雅关闭
process.on('SIGINT', async () => {
    await driver.close();
    console.log('Neo4j driver closed');
    process.exit(0);
});

// 启动服务器
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
