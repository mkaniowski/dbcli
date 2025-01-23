package cmd

import (
	"dbcli/utils"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"strconv" // only needed if you want to parse integers (for popularity or radius)
)

var taskCmd = &cobra.Command{
	Use:   "task [task number] [arguments...]",
	Short: "Executes a specific task based on the provided task number",
	// You can make this smarter if you want different Arg validation for each task
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		taskNumberStr := args[0]

		var result utils.ResultSet
		var err error

		switch taskNumberStr {
		case "1":
			// 1 argument: [1 nodeName]
			if len(args) < 2 {
				log.Fatal("Task1 requires [nodeName]")
			}
			name := args[1]
			result, err = task1(name)

		case "2":
			// 1 argument: [2 nodeName]
			if len(args) < 2 {
				log.Fatal("Task2 requires [nodeName]")
			}
			name := args[1]
			result, err = task2(name)

		case "3":
			// 1 argument: [3 nodeName]
			if len(args) < 2 {
				log.Fatal("Task3 requires [nodeName]")
			}
			name := args[1]
			result, err = task3(name)

		case "4":
			// 1 argument: [4 nodeName]
			if len(args) < 2 {
				log.Fatal("Task4 requires [nodeName]")
			}
			name := args[1]
			result, err = task4(name)

		case "5":
			// 1 argument: [5 nodeName]
			if len(args) < 2 {
				log.Fatal("Task5 requires [nodeName]")
			}
			name := args[1]
			result, err = task5(name)

		case "6":
			// 1 argument: [6 nodeName]
			if len(args) < 2 {
				log.Fatal("Task6 requires [nodeName]")
			}
			name := args[1]
			result, err = task6(name)

		case "7":
			// no arguments needed: [7]
			result, err = task7()

		case "8":
			// no arguments needed: [8]
			result, err = task8()

		case "9":
			// no arguments needed: [9]
			result, err = task9()

		case "10":
			// no arguments needed: [10]
			result, err = task10()

		case "11":
			// no arguments needed: [11]
			result, err = task11()

		case "12":
			// 2 arguments: [12 oldName newName]
			if len(args) < 3 {
				log.Fatal("Task12 requires [oldName newName]")
			}
			oldName := args[1]
			newName := args[2]
			result, err = task12(oldName, newName)

		case "13":
			// 2 arguments: [13 name newPopularity]
			if len(args) < 3 {
				log.Fatal("Task13 requires [name newPopularity]")
			}
			name := args[1]
			// parse popularity if you want an integer
			popularity, parseErr := strconv.Atoi(args[2])
			if parseErr != nil {
				log.Fatalf("popularity must be an integer: %v", parseErr)
			}
			result, err = task13(name, popularity)

		case "14":
			// 2 arguments: [14 sourceName targetName]
			if len(args) < 3 {
				log.Fatal("Task14 requires [sourceName targetName]")
			}
			sourceName := args[1]
			targetName := args[2]
			result, err = task14(sourceName, targetName)

		case "15":
			// 2 arguments: [15 sourceName targetName]
			if len(args) < 3 {
				log.Fatal("Task15 requires [sourceName targetName]")
			}
			sourceName := args[1]
			targetName := args[2]
			result, err = task15(sourceName, targetName)

		case "16":
			// 2 arguments: [16 name radius]
			if len(args) < 3 {
				log.Fatal("Task16 requires [name radius]")
			}
			name := args[1]
			radius, parseErr := strconv.Atoi(args[2])
			if parseErr != nil {
				log.Fatalf("radius must be an integer: %v", parseErr)
			}
			result, err = task16(name, radius)

		case "17":
			// 2 arguments: [17 sourceName targetName]
			if len(args) < 3 {
				log.Fatal("Task17 requires [sourceName targetName]")
			}
			sourceName := args[1]
			targetName := args[2]
			result, err = task17(sourceName, targetName)

		case "18":
			// Expecting 2 arguments: [18 sourceName targetName]
			if len(args) < 3 {
				log.Fatal("Task18 requires [sourceName targetName]")
			}
			sourceName := args[1]
			targetName := args[2]
			result, err = task18(sourceName, targetName)

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

// 1. finds all children of a given node
func task1(name string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`SELECT expand(out()) FROM V WHERE name = "%s"`, name)
	return utils.ExecuteQuery(query)
}

// 2. counts all children of a given node
func task2(name string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`SELECT out().size() FROM V WHERE name = "%s"`, name)
	return utils.ExecuteQuery(query)
}

// 3. finds all grandchildren of a given node
func task3(name string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`SELECT expand(out()).out() FROM V WHERE name = "%s"`, name)
	return utils.ExecuteQuery(query)
}

// 4. finds all parents of a given node
func task4(name string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`SELECT expand(in()) FROM V WHERE name = "%s"`, name)
	return utils.ExecuteQuery(query)
}

// 5. counts all parents of a given node
func task5(name string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`SELECT in().size() FROM V WHERE name = "%s"`, name)
	return utils.ExecuteQuery(query)
}

// 6. finds all grandparents of a given node
func task6(name string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`SELECT expand(in().in()) FROM V WHERE name = "%s"`, name)
	return utils.ExecuteQuery(query)
}

// 7. counts how many distinct node names exist
func task7() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`SELECT count(distinct(name)) FROM V`)
}

// 8. finds nodes that are not a subcategory of any other node
func task8() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`SELECT * FROM V WHERE in().size() = 0`)
}

// 9. counts how many nodes satisfy task8()
func task9() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`SELECT count(*) FROM V WHERE in().size() = 0`)
}

// 10. finds nodes with the largest number of children
func task10() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`
        SELECT FROM V
        WHERE out().size() = (
            SELECT max(out().size()) FROM V
        )
    `)
}

// 11. finds nodes with the smallest number of children (greater than zero)
func task11() (utils.ResultSet, error) {
	return utils.ExecuteQuery(`
        SELECT FROM V
        WHERE out().size() = (
            SELECT min(out().size())
            FROM V
            WHERE out().size() > 0
        )
    `)
}

// 12. changes the name of a given node (oldName -> newName)
func task12(oldName, newName string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`UPDATE V SET name = '%s' WHERE name = '%s'`, newName, oldName)
	return utils.ExecuteQuery(query)
}

// 13. changes the popularity of a given node
func task13(name string, popularity int) (utils.ResultSet, error) {
	// If popularity should remain a string, adjust to %s instead of %d
	query := fmt.Sprintf(`UPDATE V SET popularity = %d WHERE name = '%s'`, popularity, name)
	return utils.ExecuteQuery(query)
}

// 14. finds all paths (up to depth 6) from sourceName to anything except targetName
func task14(sourceName, targetName string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`
        SELECT expand(both()) FROM (
            TRAVERSE out() 
            FROM (SELECT FROM V WHERE name = "%s")
            WHILE $depth <= 6
              AND @rid != (SELECT @rid FROM V WHERE name = "%s")
        )`, sourceName, targetName)
	return utils.ExecuteQuery(query)
}

// 15. counts nodes on all paths (up to depth 6) from sourceName to anything except targetName
func task15(sourceName, targetName string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`
        SELECT count(*) FROM (
            TRAVERSE out() 
            FROM (SELECT FROM V WHERE name = "%s")
            WHILE $depth <= 6
              AND @rid != (SELECT @rid FROM V WHERE name = "%s")
        )`, sourceName, targetName)
	return utils.ExecuteQuery(query)
}

// 16. calculates popularity in the neighborhood (up to 'radius') of the given node
func task16(name string, radius int) (utils.ResultSet, error) {
	query := fmt.Sprintf(`
        SELECT sum(popularity) FROM (
            TRAVERSE both() 
            FROM (SELECT FROM V WHERE name = "%s")
            WHILE $depth <= %d
        )`, name, radius)
	return utils.ExecuteQuery(query)
}

// 17. calculates popularity on the shortest path between two given nodes
func task17(sourceName, targetName string) (utils.ResultSet, error) {
	query := fmt.Sprintf(`
        SELECT sum(popularity) FROM (
            SELECT expand(path) FROM (
                SELECT shortestPath(
                    (SELECT FROM V WHERE name = '%s'),
                    (SELECT FROM V WHERE name = '%s')
                ) AS path
            ) UNWIND path
        )`, sourceName, targetName)
	return utils.ExecuteQuery(query)
}

// 18. finds the directed path with the greatest total popularity
// between two given nodes (sourceName -> targetName)
func task18(sourceName, targetName string) (utils.ResultSet, error) {
	/*
	   Explanation:

	   1) We call allSimplePaths() from the 'sourceName' to the 'targetName'
	      with direction "OUT" so that we only follow outgoing edges.

	   2) UNWIND each path (so we can sum popularity).

	   3) For each path, compute sum_popularity = sum of all node popularities in it.

	   4) ORDER BY sum_popularity DESC, pick the top path (LIMIT 1).

	   5) Finally, expand that top path so you see the actual node records in the result.
	*/

	query := fmt.Sprintf(`
        SELECT expand(path) 
        FROM (
            SELECT path 
            FROM (
                SELECT 
                    path,
                    (SELECT sum(popularity) FROM (SELECT expand(path))) AS sum_popularity
                FROM (
                    SELECT allSimplePaths(
                        (SELECT FROM V WHERE name = '%s'),
                        (SELECT FROM V WHERE name = '%s'),
                        {
                            maxDepth: 50,
                            direction: 'OUT'
                        }
                    ) AS path
                ) 
                UNWIND path
            )
            ORDER BY sum_popularity DESC
            LIMIT 1
        )
    `, sourceName, targetName)

	return utils.ExecuteQuery(query)
}
