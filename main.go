package main

import (
	"bufio"
	"errors"
	"fmt"
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
	NumOfRows int
	Pages     [TABLE_MAX_PAGES]*Page
}

type Page struct {
	Data [PAGE_SIZE]byte
}

type Row struct {
	Id       uint32
	Username string
	Email    string
}

func main() {
	t := Table{}
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

			code, err = ExecuteStatement(&st, &t)

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
	for i := 0; i < t.NumOfRows; i++ {
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

func RowSlot(t *Table, n int) (*Page, int) {
	pageNum := n / ROWS_PER_PAGE
	page := t.Pages[pageNum]
	if page == nil {
		t.Pages[pageNum] = new(Page)
		page = t.Pages[pageNum]
	}
	rowOffset := n % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return page, byteOffset
}

func I32ToB(p *Page, offset int, num uint32) {
	for i := 0; i < 4; i++ {
		p.Data[offset] = byte((num >> (8 * i)) & 0xff)
		offset++
	}
}

func BToI32(p *Page, offset int) (res uint32) {
	for i := 0; i < 4; i++ {
		res |= uint32(p.Data[offset]) << (8 * i)
		offset++
	}
	return
}

func BCopy(p *Page, offset, size int, b []byte) {
	for i := 0; i < size; i++ {
		if i == len(b) {
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

func DeserializeRow(p *Page, offset int) Row {
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
