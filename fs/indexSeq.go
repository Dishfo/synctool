package fs

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
)

const (
	insertInfoSeq = `insert into InfoSeq (seqs,folder) 
	 values (?,?) `

	selectInfoSeqById = `select id,seqs,folder from InfoSeq
	 where id = ? 	     
	`

	selectUpdate = `
	select id,seqs,folder from InfoSeq 
	where id > ? and folder = ? 
	`
)

type IndexSeq struct {
	Id     int64
	Seq    []int64
	Folder string
}

/**
db funtions with tx
*/

func StoreIndexSeq(tx *sql.Tx, seq IndexSeq) (int64, error) {
	seqs := []string{}
	for _, n := range seq.Seq {
		seqs = append(seqs, strconv.FormatInt(n, 10))
	}
	res, err :=
		tx.Exec(insertInfoSeq, strings.Join(seqs, ","), seq.Folder)
	if err != nil {
		log.Printf("%s when insert a seq %s", err.Error(), seqs)
		return -1, err
	} else {
		id, _ := res.RowsAffected()
		return id, nil
	}
}

func GetIndexSeqAfter(tx *sql.Tx, id int64, folder string) ([]*IndexSeq, error) {
	rows, err := tx.Query(selectUpdate, id, folder)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	indexSeqs := make([]*IndexSeq,0)
	for rows.Next() {
		indexSeq := new(IndexSeq)
		fillIndexSeq(rows,indexSeq)
		indexSeqs = append(indexSeqs, indexSeq)
	}

	return indexSeqs, nil
}

func fillIndexSeq(rows *sql.Rows, indexSeq *IndexSeq) {
	var seqs string
	_ = rows.Scan(&indexSeq.Id, &seqs, &indexSeq.Folder)
	seqarray := strings.Split(seqs, ",")
	seqnums := make([]int64, 0)
	for _, s := range seqarray {
		num, _ := strconv.ParseInt(s, 10, 64)
		seqnums = append(seqnums, num)
	}
	indexSeq.Seq = seqnums

}

func GetIndexSeq(tx *sql.Tx, id int64) (*IndexSeq, error) {
	rows, err := tx.Query(selectInfoSeqById, id)
	if err != nil {
		_ = tx.Rollback()
		log.Printf("%s when select indexSeq by %d ", err.Error(), id)
	}
	defer rows.Close()
	if rows.Next() {
		indexSeq := new(IndexSeq)
		fillIndexSeq(rows, indexSeq)
		return indexSeq, nil
	} else {
		return nil, nil
	}
}
