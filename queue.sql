CREATE TABLE IF NOT EXISTS "queue" (
	"job_id"	INTEGER,
	"created_timestamp"	INTEGER NOT NULL,
	"locked_timestamp"	INTEGER,
	"reset_count"	INTEGER NOT NULL DEFAULT 0,
	"queue_name"	TEXT NOT NULL,
	"job_input"	BLOB NOT NULL,
	"job_output"	BLOB,
	"job_status" INTEGER NOT NULL DEFAULT 0, /* 0=new, 1=running, 2=ready, 3=failed */
	PRIMARY KEY("job_id" AUTOINCREMENT)
);

CREATE INDEX IF NOT EXISTS "created" ON "queue" (
	"created_timestamp"	ASC
);

CREATE INDEX IF NOT EXISTS "done" ON "queue" (
	"job_done"
);
