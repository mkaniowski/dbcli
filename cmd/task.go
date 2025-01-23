package cmd

import (
	"dbcli/utils"
	"github.com/spf13/cobra"
	"log"
)

var taskCmd = &cobra.Command{
	Use:   "task [task number]",
	Short: "Executes a specific task based on the provided task number",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		taskNumberStr := args[0]

		var result utils.ResultSet
		var err error

		switch taskNumberStr {
		case "1":
			result, err = task1()
		case "2":
			result, err = task2()
		case "3":
			result, err = task3()
		case "4":
			result, err = task4()
		case "5":
			result, err = task5()
		case "6":
			result, err = task6()
		case "7":
			result, err = task7()
		case "8":
			result, err = task8()
		case "9":
			result, err = task9()
		case "10":
			result, err = task10()
		case "11":
			result, err = task11()
		case "12":
			result, err = task12()
		case "13":
			result, err = task13()
		case "14":
			result, err = task14()
		case "15":
			result, err = task15()
		case "16":
			result, err = task16()
		case "17":
			result, err = task17()
		case "all":
			result, err = task1()
			result, err = task2()
			result, err = task3()
			result, err = task4()
			result, err = task5()
			result, err = task6()
			result, err = task7()
			result, err = task8()
			result, err = task9()
			result, err = task10()
			result, err = task11()
			result, err = task12()
			result, err = task13()
			result, err = task14()
			result, err = task15()
			result, err = task16()
			//result, err = task17()
		default:
			log.Fatalf("Invalid task number: %s. Please provide a number between 1 and 17.", taskNumberStr)
		}

		if err != nil {
			log.Fatalf("Failed to execute Task%s: %v", taskNumberStr, err)
		}

		log.Printf("Response Body for Task%s: %s", taskNumberStr, result)
	},
}

func init() {
	rootCmd.AddCommand(taskCmd)
}

func task1() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT expand(out()) FROM V WHERE name = \"Planned_cities_by_country\"")
}

func task2() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT out().size() FROM V WHERE name = \"Planned_cities_by_country\"")
}

func task3() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT expand(out()).out() FROM V WHERE name = \"Planned_cities_by_country\"")
}

func task4() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT expand(in()) FROM V WHERE name = \"Planned_cities_by_country\"")
}

func task5() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT in().size() FROM V WHERE name = \"Planned_cities_by_country\"")
}

func task6() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT expand(in().in()) FROM V WHERE name = \"Planned_cities_by_country\"")
}

func task7() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT count(distinct(name)) FROM V")
}

func task8() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT * FROM V WHERE in().size() = 0")
}

func task9() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT count(*) FROM V WHERE in().size() = 0")
}

func task10() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT FROM V WHERE out().size() = (SELECT max(out().size()) FROM V)")
}

func task11() (utils.ResultSet, error) {
	return utils.ExecuteQuery("SELECT FROM V WHERE out().size() = (SELECT min(out().size()) FROM V WHERE out().size() > 0)")
}

func task12() (utils.ResultSet, error) {
	return utils.ExecuteQuery("UPDATE V SET name = 'xfafafafa' WHERE name = 'Planned_cities_by_country'")
}

func task13() (utils.ResultSet, error) {
	return utils.ExecuteQuery("UPDATE V SET popularity = 13213131 WHERE name = 'xfafafafa'")
}

func task14() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`SELECT expand(both()) FROM (
    TRAVERSE out() FROM (SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues')
    WHILE $depth <= 6 AND @rid != (SELECT @rid FROM V WHERE name = 'Christianity_in_Bolivia')
    )`)
}

func task15() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`SELECT count(*) FROM (
    TRAVERSE out() FROM (SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues')
    WHILE $depth <= 6 AND @rid != (SELECT @rid FROM V WHERE name = 'Christianity_in_Bolivia')
    )`)
}

func task16() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`SELECT sum(popularity) FROM (
    TRAVERSE both() FROM (SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues')
    WHILE $depth <= 6
    )`)
}

func task17() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`SELECT sum(popularity) FROM
    (
    SELECT expand(path) FROM (
    SELECT (shortestPath((SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues'), (SELECT FROM V WHERE name = 'December_1976_sports_events_in_Europe'))) as path
    ) UNWIND path
    )`)
}
