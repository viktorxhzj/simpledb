package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type (
	StatementType string
)

const (
	SELECT StatementType = "SELECT"
	INSERT StatementType = "INSERT"
)

const (
	META_EXIT = 0
	SUCCESS   = 1

	UNKNOWN           = -1
	TABLE_FULL        = -2
	INVALID_STATEMENT = -3
)

const (
	USERNAME_SIZE   = 32
	EMAIL_SIZE      = 255
	ROW_SIZE        = 12 + USERNAME_SIZE + EMAIL_SIZE
	PAGE_SIZE       = 4096
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_PAGES = 100
	TABLE_MAX_ROWS  = TABLE_MAX_PAGES * ROWS_PER_PAGE
)

var (
	UnknownMetaErr      = errors.New("unknown meta commands!")
	UnknownStatementErr = errors.New("unknown statement commands!")
	LongStringErr       = errors.New("input strings are too long!")
	TableFullErr        = errors.New("the table is full!")
)

type Statement struct {
	Type        StatementType
	RowToInsert Row
}

type Table struct {
	NumOfRows uint32
	Pager
}

type Page struct {
	Data [PAGE_SIZE]byte
}

type Pager struct {
	*os.File
	FileLength uint32
	Pages     [TABLE_MAX_PAGES]*Page
}

func (p *Pager) GetPage(pageNum uint32) *Page {
	if p.Pages[pageNum] == nil {
		p.Pages[pageNum] = &Page{}
		numOfPages := p.FileLength / PAGE_SIZE

		if (p.FileLength % PAGE_SIZE) != 0 {
			numOfPages++
		}

		if pageNum <= numOfPages {
			b := make([]byte, PAGE_SIZE)
			n, err := p.File.ReadAt(b, int64(pageNum) * PAGE_SIZE)
			if err != nil {
				fmt.Println("read page", n, err)
			}
			for i := 0; i < n; i++ {
				p.Pages[pageNum].Data[i] = b[i]
			}
		}
	}
	return p.Pages[pageNum]
}

func (p *Pager) FlushPage(pageNum, size uint32) {
	fmt.Println("Flush addi", pageNum, size)

	b := p.Pages[pageNum].Data[:size]

	n, err := p.File.WriteAt(b, int64(pageNum) * PAGE_SIZE)

	if err != nil {
		fmt.Println("flush page", n, err)
	}
}

type Row struct {
	Id       uint32
	Username string
	Email    string
}

func OpenDB(name string) *Table {

	t := new(Table)
	t.CreatePager(name)
	t.NumOfRows = t.Pager.FileLength / ROW_SIZE

	return t
}

func CloseDB(t *Table) {
	numOfFullPages := t.NumOfRows / ROWS_PER_PAGE

	for i := uint32(0); i < numOfFullPages; i++ {
		if t.Pager.Pages[i] == nil {
			continue
		}

		t.Pager.FlushPage(i, PAGE_SIZE)
	}

	if numOfAdditionalRows := t.NumOfRows % ROWS_PER_PAGE; numOfAdditionalRows > 0 {
		pageNum := numOfFullPages

		if t.Pager.Pages[pageNum] != nil {
			t.Pager.FlushPage(pageNum, numOfAdditionalRows * ROW_SIZE)
		}
	}

}

func (t *Table) CreatePager(name string) {
	t.Pager.File, _ = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	
	l, err := t.Pager.File.Seek(0, io.SeekEnd)

	if err != nil {
		fmt.Println("create pager", err)
	}

	t.Pager.FileLength = uint32(l)
}


func main() {

	t := OpenDB("mydb")

	// if len(os.Args) < 2 {
	// 	panic("invalid argument")
	// }

	// t := OpenDB(os.Args[1])

	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Database Shell\n----------------")

repl:
	for {
		fmt.Print("-> ")

		str, _ := reader.ReadString('\n')
		str = strings.TrimSuffix(str, "\n")

		if str[0] == '.' {
			code, err := CheckMetaCommand(str)

			if err != nil {
				fmt.Println(code, err)
				continue repl
			}

			switch code {
			case META_EXIT:
				fmt.Println("-> exiting database...")
				CloseDB(t)
				break repl

			default:
			}
		} else {
			var st Statement

			code, err := PrepareStatement(str, &st)

			if err != nil {
				fmt.Println(code, err)
				continue repl
			}

			code, err = ExecuteStatement(&st, t)

			if err != nil {
				fmt.Println(code, err)
				continue repl
			}
		}

		fmt.Println("-> executed")

	}
}

func CheckMetaCommand(str string) (int, error) {
	if str == ".exit" {
		return META_EXIT, nil
	}
	return UNKNOWN, UnknownMetaErr
}

func PrepareStatement(str string, st *Statement) (int, error) {
	if strings.HasPrefix(str, "select") {
		st.Type = SELECT
		return SUCCESS, nil
	}

	if strings.HasPrefix(str, "insert") {
		st.Type = INSERT
		fmt.Sscanf(str, "insert %d %s %s", &st.RowToInsert.Id, &st.RowToInsert.Username, &st.RowToInsert.Email)

		if len([]byte(st.RowToInsert.Username)) > USERNAME_SIZE || len([]byte(st.RowToInsert.Email)) > EMAIL_SIZE {
			return INVALID_STATEMENT, LongStringErr
		}

		return SUCCESS, nil
	}

	return UNKNOWN, UnknownStatementErr
}

func ExecuteStatement(st *Statement, t *Table) (int, error) {
	switch st.Type {
	case SELECT:
		return ExecuteSelect(t)
	case INSERT:
		return ExecuteInsert(st.RowToInsert, t)
	}

	return SUCCESS, nil
}

func ExecuteSelect(t *Table) (int, error) {
	fmt.Println("Row\tId\tUsername\tEmail")
	for i := uint32(0); i < t.NumOfRows; i++ {
		r := DeserializeRow(RowSlot(t, i))
		fmt.Printf("(%d\t%d\t%s\t%s)\n", i, r.Id, r.Username, r.Email)
	}
	return SUCCESS, nil
}

func ExecuteInsert(r Row, t *Table) (int, error) {
	if t.NumOfRows > TABLE_MAX_ROWS {
		return TABLE_FULL, TableFullErr
	}
	SerializeRow(r, t)
	t.NumOfRows++
	return SUCCESS, nil
}

func RowSlot(t *Table, n uint32) (*Page, uint32) {
	pageNum := n / ROWS_PER_PAGE
	page := t.Pager.GetPage(pageNum)
	rowOffset := n % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return page, byteOffset
}

func I32ToB(p *Page, offset uint32, num uint32) {
	for i := 0; i < 4; i++ {
		p.Data[offset] = byte((num >> (8 * i)) & 0xff)
		offset++
	}
}

func BToI32(p *Page, offset uint32) (res uint32) {
	for i := 0; i < 4; i++ {
		res |= uint32(p.Data[offset]) << (8 * i)
		offset++
	}
	return
}

func BCopy(p *Page, offset, size uint32, b []byte) {
	for i := uint32(0); i < size; i++ {
		if i == uint32(len(b)) {
			break
		}
		p.Data[offset] = b[i]
		offset++
	}
}

func SerializeRow(r Row, t *Table) {
	p, offset := RowSlot(t, t.NumOfRows)

	b1, b2 := []byte(r.Username), []byte(r.Email)
	l1, l2 := len(b1), len(b2)

	I32ToB(p, offset, uint32(l1))
	offset += 4

	I32ToB(p, offset, uint32(l2))
	offset += 4

	I32ToB(p, offset, r.Id)
	offset += 4

	BCopy(p, offset, USERNAME_SIZE, b1)
	offset += USERNAME_SIZE

	BCopy(p, offset, EMAIL_SIZE, b2)
}

func DeserializeRow(p *Page, offset uint32) Row {
	var r Row

	l1 := BToI32(p, offset)
	offset += 4

	l2 := BToI32(p, offset)
	offset += 4

	r.Id = BToI32(p, offset)
	offset += 4

	b1 := p.Data[offset : uint32(offset)+l1]
	offset += USERNAME_SIZE

	b2 := p.Data[offset : uint32(offset)+l2]

	r.Username = string(b1)
	r.Email = string(b2)

	return r
}
