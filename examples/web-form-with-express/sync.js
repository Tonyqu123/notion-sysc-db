const { Client } = require("@notionhq/client")
const sqlite3 = require("sqlite3").verbose()
const dotenv = require("dotenv")
const cron = require("node-cron")

dotenv.config()

// 初始化 Notion 客户端
const notion = new Client({ auth: process.env.NOTION_KEY })

// 连接到 SQLite 数据库
const dbPath = "/Users/i519593/code/youtube-downloader/downloads.db"

const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('无法连接到 SQLite 数据库:', err.message)
    process.exit(1)
  }
  console.log('已连接到 SQLite 数据库.')
})

// 创建同步状态表（如果尚未存在）
db.run(`
  CREATE TABLE IF NOT EXISTS sync_status (
    id INTEGER PRIMARY KEY,
    last_sync TIMESTAMP
  )
`, (err) => {
  if (err) {
    console.error('无法创建 sync_status 表:', err.message)
  }
})

// 获取上次同步时间
const getLastSyncTime = () => {
  return new Promise((resolve, reject) => {
    db.get('SELECT last_sync FROM sync_status WHERE id = 1', (err, row) => {
      if (err) {
        return reject(err)
      }
      resolve(row ? row.last_sync : null)
    })
  })
}

// 更新最后同步时间
const updateLastSyncTime = (time) => {
  return new Promise((resolve, reject) => {
    db.run(`
      INSERT INTO sync_status (id, last_sync)
      VALUES (1, ?)
      ON CONFLICT(id) DO UPDATE SET last_sync = excluded.last_sync
    `, [time], function(err) {
      if (err) {
        return reject(err)
      }
      resolve()
    })
  })
}

// 查询新增数据
const fetchNewData = async (lastSync) => {
  return new Promise((resolve, reject) => {
    let query = 'SELECT * FROM downloads WHERE id > ?'
    db.all(query, [lastSync || '14650d7b-5b6c-4b13-bba5-d8ebce1061dc'], (err, rows) => {
      if (err) {
        return reject(err)
      }
      resolve(rows)
    })
  })
}

// 批量插入数据到 Notion
const insertToNotion = async (data) => {
  const batchSize = 10
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize)
    const promises = batch.map(item => {
      return notion.pages.create({
        parent: { database_id: process.env.NOTION_DATABASE_ID },
        properties: {
          name: {
            title: [
              {
                text: {
                  content: item.name || '',
                },
              },
            ],
          },
          abstract: {
            rich_text: [
              {
                text: {
                  content: item.abstract || '',
                },
              },
            ],
          },
          file_path: {
            rich_text: [
              {
                text: {
                  content: item.file_path || '',
                },
              },
            ],
          },
        },
      }).catch(err => {
        console.error(`插入 Notion 页面失败，ID: ${item.id}`, err)
        // 这里可以记录失败的项以便后续重试
      })
    })
    await Promise.all(promises)
    // 考虑 Notion API 的速率限制
    await new Promise(resolve => setTimeout(resolve, 1000))
  }
}

// 同步函数
const syncData = async () => {
  try {
    const lastSync = await getLastSyncTime()
    console.log('上次同步时间:', lastSync)
    const newData = await fetchNewData(lastSync)
    if (newData.length === 0) {
      console.log('没有新数据需要同步.')
      return
    }
    console.log(`同步 ${newData.length} 条新数据到 Notion.`)
    await insertToNotion(newData)
    const currentTime = new Date().toISOString()
    await updateLastSyncTime(currentTime)
    console.log('同步完成，更新时间:', currentTime)
  } catch (error) {
    console.error('同步过程中出错:', error)
  }
}

// 设置定时任务，每小时同步一次
cron.schedule('0 * * * *', () => {
  console.log('开始同步任务...')
  syncData()
})

// 启动时立即运行一次同步
syncData() 