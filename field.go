package gms

type field struct {
	// Stores the MySQL type of this field. For example, the VARCHAR type.
	ftype fieldType

	// Stores the MySQL flags for this field. For example, the NOT NULL flag.
	flag fieldFlag

	// Stores the MySQL field name for this field.
	name string
}
