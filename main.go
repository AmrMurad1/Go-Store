package main

import (
	"fmt"
)

func main() {
	db, err := NewEngine("./data")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	db.Set("name", "john")
	db.Set("age", "25")
	db.Set("city", "paris")
	db.Set("country", "france")
	db.Set("job", "engineer")
	db.Set("company", "tech-corp")
	db.Set("salary", "75000")
	db.Set("department", "backend")
	db.Set("level", "senior")
	db.Set("experience", "5years")
	db.Set("skills", "go,python,sql")
	db.Set("education", "masters")
	db.Set("university", "sorbonne")
	db.Set("hobby", "reading")
	db.Set("sport", "tennis")
	db.Set("music", "jazz")
	db.Set("food", "italian")
	db.Set("color", "green")
	db.Set("season", "spring")
	db.Set("pet", "cat")

	db.Set("name", "alice")
	db.Set("job", "developer")

	val, err := db.Get("name")
	if err == nil {
		fmt.Println("name:", val)
	} else {
		fmt.Println("Error:", err)
	}

	val, err = db.Get("job")
	if err == nil {
		fmt.Println("job:", val)
	} else {
		fmt.Println("Error:", err)
	}

	db.Delete("age")
	val, err = db.Get("age")
	if err == nil {
		fmt.Println("age:", val)
	} else {
		fmt.Println("age deleted:", err)
	}
}
