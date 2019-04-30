package syncfile

import (
	"log"
	"testing"
	"time"
)

func TestStorage(t *testing.T) {
	log.Println("ok")

outter:
	for {
		log.Println(1)
		select {
		default:
			log.Println(1)
			continue outter
			log.Println(2)
		}
	}
}

func TestTaskManger(t *testing.T) {
	tm := NewTaskManager()

	tm.AddTask(Task{
		Type: TASK_DUR,
		Dur:  int64(time.Second * 5),
		Act: func() {
			log.Println("123456")
		},
	})
	timer := time.NewTimer(time.Second * 20)
	select {
	case <-timer.C:
		tm.Close()
	}

	select {}
}

func TestFileOp(t *testing.T) {
	bak, err := deleteFile("/home/dishfo/test2/fileA", true)
	if err != nil {
		log.Fatal(err)
	}
	restoreBak(bak)

	bak, err = deleteFolder("/home/dishfo/test2/dir1", true)
	if err != nil {
		log.Fatal(err)
	}
	restoreBak(bak)

}

const (
	table1 = ` 
	create table test1 (
	id integer not null primary key autoincrement,
	folder text
	)
	`
	table2 = `create table test2 (
	id integer not null primary key autoincrement,
	folder text
	)
	`

	i1 = `insert into test1 (folder) values (?) `
	i2 = `insert into test1 (folder) values (?) `
)

//使用的锁是库级锁
func TestSqlite(t *testing.T) {
	db, err := initDB("test.db")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	db.Exec(table1)
	db.Exec(table2)
	go func() {
		for {
			tx, err := db.Begin()
			if err != nil {
				log.Fatalln(err)
			}

			_, err = tx.Exec(i1, "12345")
			if err != nil {
				log.Fatalln(err)
			}

			err = tx.Commit()
			if err != nil {
				log.Fatalf("%s when insert into test1",
					err.Error())
			}

		}

	}()

	go func() {
		for {
			tx, err := db.Begin()
			if err != nil {
				log.Fatalln(err)
			}

			_, err = tx.Exec(i2, "12345")
			if err != nil {
				log.Fatalln(err)
			}

			err = tx.Commit()
			if err != nil {
				log.Fatalf("%s when insert into test1",
					err.Error())
			}

		}
	}()

	select {}
}
