use crate::db::Database;
use rusqlite::{params, Result, Row};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub file_id: String,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: String,
}

fn row_to_job(row: &Row) -> Result<Job> {
    Ok(Job {
        id: row.get(0)?,
        file_id: row.get(1)?,
        status: row.get(2)?,
        error_message: row.get(3)?,
        created_at: row.get(4)?,
    })
}

pub struct JobRepository<'db> {
    db: &'db Database,
}

impl<'db> JobRepository<'db> {
    pub fn new(db: &'db Database) -> Self {
        Self { db }
    }

    pub fn insert_job(&self, file_id: &str) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        self.db.conn.execute(
            "INSERT INTO jobs (id, file_id, status, error_message) VALUES (?1, ?2, ?3, ?4)",
            params![&id, file_id, "pending", None::<String>],
        )?;
        Ok(id)
    }

    pub fn get_jobs_by_file_id(&self, file_id: &str, status: &str) -> Result<Vec<Job>> {
        let base_query = "SELECT id, file_id, status, error_message, created_at FROM jobs WHERE file_id = ?1";
        let jobs = if status.eq_ignore_ascii_case("any") {
            let mut stmt = self.db.conn.prepare(base_query)?;
            stmt.query_map(params![file_id], row_to_job)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            let full_query = format!("{} AND status = ?2", base_query);
            let mut stmt = self.db.conn.prepare(&full_query)?;
            stmt.query_map(params![file_id, status], row_to_job)?
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(jobs)
    }

    pub fn get_jobs(&self, status: &str) -> Result<Vec<Job>> {
        let mut stmt = self.db.conn.prepare(
            "SELECT id, file_id, status, error_message, created_at FROM jobs WHERE status = ?1",
        )?;
        let jobs = stmt
            .query_map([status], row_to_job)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(jobs)
    }

    pub fn update_job_batch(&self, job_ids: Vec<String>, status: &str, error_message: Option<&str>) -> Result<()> {
        let mut sql = String::new();
        for job_id in &job_ids {
            let err_val = match error_message {
                Some(msg) => format!("'{}'", msg.replace('\'', "''")),
                None => "NULL".to_string(),
            };
            sql.push_str(&format!(
                "UPDATE jobs SET status = '{}', error_message = {} WHERE id = '{}';",
                status.replace('\'', "''"),
                err_val,
                job_id.replace('\'', "''"),
            ));
        }
        self.db.conn.execute_batch(&sql)
    }

    pub fn get_queue_size(&self) -> Result<usize> {
        let count: i64 =
            self.db.conn
                .query_row("SELECT COUNT(*) FROM jobs WHERE status = 'pending'", [], |row| {
                    row.get(0)
                })?;

        Ok(count as usize)
    }
}
