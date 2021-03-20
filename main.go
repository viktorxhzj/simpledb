package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type (
	CodeEnum int
	StatementType string
)

const PROMPT = "-> "

const (
	SELECT StatementType = "SELECT"
	INSERT StatementType = "INSERT"
)

const (
	META_EXIT CodeEnum = 0
	META_UNKNOWN CodeEnum = -1
	
	PREPARE_SUCCESS CodeEnum = 1
	PREPARE_UNKNOWN CodeEnum = -2
)

type Statement struct {
	Type StatementType
}


func main() {

	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Database Shell\n----------------")

	repl: for {
		fmt.Print(PROMPT)

		str, _ := reader.ReadString('\n')
		str = strings.TrimSuffix(str, "\n")

		if str[0] == '.' {
			switch CheckMetaCommand(str) {
			case META_EXIT:
				fmt.Println("-> exiting database...")
				break repl
			case META_UNKNOWN:
				fmt.Println("-> unknown meta command!")
				continue repl
			}
		}

		var st Statement

		switch PrepareStatement(str, &st) {
		case PREPARE_SUCCESS:
		case PREPARE_UNKNOWN:
			fmt.Println("-> unknown sql statement!")
			continue repl			
		}
		
		ExecuteStatement(&st)

		fmt.Println("-> executed")

	}
}

func CheckMetaCommand(str string) CodeEnum {
	if str == ".exit" {
		return META_EXIT
	}
	return META_UNKNOWN
}

func PrepareStatement(str string, st *Statement) CodeEnum {
	if strings.HasPrefix(str, "select") || strings.HasPrefix(str, "SELECT") {
		st.Type = SELECT
		return PREPARE_SUCCESS
	}

	if strings.HasPrefix(str, "select") || strings.HasPrefix(str, "SELECT") {
		st.Type = INSERT
		return PREPARE_SUCCESS
	}
	
	return PREPARE_UNKNOWN
}

func ExecuteStatement(st *Statement) {
	switch st.Type {
	case SELECT:
	case INSERT:
	}
}