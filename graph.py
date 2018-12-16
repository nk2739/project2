import click
from google.cloud import bigquery

uni1 = 'nk2739'  # Your uni
uni2 = 'cr3014'  # Partner's uni. If you don't have a partner, put None

# Test function


def testquery(client):
    q = """
        """
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 1. You must edit this funtion.
# This function should return a list of IDs and the corresponding text.


def q1(client):
    q = """
        SELECT id, text FROM `w4111-columbia.graph.tweets` 
        WHERE text LIKE '%going live%' AND text LIKE '%www.twitch%'
        """

    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding
# average likes.


def q2(client):
    q = """
    	SELECT SUBSTR(create_time,0,STRPOS(create_time,' ')-1) AS day, avg(like_num) AS avg_likes 
    	FROM `w4111-columbia.graph.tweets`
        GROUP BY day ORDER BY avg_likes DESC LIMIT 1
        """

    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes
# in the dataset.GRAPH.


def q3(client):

    q = """
        CREATE OR REPLACE TABLE dataset.GRAPH AS
    	SELECT twitter_username AS src, REGEXP_EXTRACT(text, r'@([^\s]+)') AS dst 
    	FROM `w4111-columbia.graph.tweets`
        WHERE REGEXP_EXTRACT(text, r'@([^\s]+)') IS NOT NULL GROUP BY src, dst
        """

    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of
# the users having the max indegree and max outdegree.


def q4(client):
    q = """ 
    	SELECT * 
    	FROM (SELECT dst AS max_indegree FROM dataset.GRAPH GROUP BY dst ORDER BY COUNT(*) DESC LIMIT 1), 
             (SELECT src AS max_outdegree FROM dataset.GRAPH GROUP BY src ORDER BY COUNT(*) DESC LIMIT 1)
        """

    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional
# probability.


def q5(client):
    q1 = """ 
        CREATE OR REPLACE TABLE dataset.all_nodes AS (
    		SELECT src AS node FROM dataset.GRAPH
    		union distinct
    		SELECT dst AS node FROM dataset.GRAPH
    	)
        """

    q2 = """
        WITH indegrees AS (
    		SELECT dst, count(*) AS indegree FROM dataset.GRAPH GROUP BY dst
    	), avg_like_nums AS (
    		SELECT twitter_username, avg(like_num) AS avg_like_num 
            FROM `w4111-columbia.graph.tweets` GROUP BY twitter_username
    	), merge_table AS (
   	 		SELECT node, CASE WHEN indegree IS NULL THEN 0 ELSE indegree END AS indegree, 
                        CASE WHEN  avg_like_num IS NULL THEN 0 ELSE avg_like_num END AS avg_like_num
    		FROM (dataset.all_nodes LEFT OUTER JOIN indegrees ON node = dst) 
                    LEFT OUTER JOIN avg_like_nums ON node = twitter_username 
    	), popular AS (
    		SELECT node FROM merge_table AS M
    		WHERE M.indegree >= (SELECT avg(indegree) FROM indegrees) 
            AND M.avg_like_num >= (SELECT avg(like_num) FROM `w4111-columbia.graph.tweets`)
    	), unpopular AS (
    	    SELECT node FROM merge_table AS M
    		WHERE M.indegree < (SELECT avg(indegree) FROM indegrees) 
            AND M.avg_like_num < (SELECT avg(like_num) FROM `w4111-columbia.graph.tweets`)
    	)
    	SELECT pop_and_unpop/unpop as popular_unpopular
    	FROM (SELECT count(*) AS pop_and_unpop FROM `w4111-columbia.graph.tweets` 
                WHERE twitter_username IN (SELECT * FROM unpopular) 
                AND REGEXP_EXTRACT(text, r'@([^\s]+)') IN (SELECT * FROM popular)),
    		 (SELECT count(*) AS unpop FROM `w4111-columbia.graph.tweets` 
                WHERE twitter_username IN (SELECT * FROM unpopular))
    	"""
    job = client.query(q1)
    job.result()
    job = client.query(q2)
    results = job.result()
    return list(results)

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number
# of triangles in the dataset.GRAPH.


def q6(client):
    q = """
		SELECT count(*)/3 AS no_of_triangles
		FROM (SELECT * FROM dataset.GRAPH AS G1, dataset.GRAPH AS G2, dataset.GRAPH AS G3
			  WHERE G1.dst = G2.src AND G2.dst = G3.src AND G3.dst = G1.src 
              AND G1.src <> G1.dst AND G2.src <> G2.dst AND G3.src <> G3.dst)
		"""

    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and
# their corresponding PageRank.


def q7(client):
    q1 = """
        CREATE OR REPLACE TABLE dataset.pageranks AS 
        SELECT node as twitter_username, page_rank_score 
        FROM dataset.all_nodes, (SELECT 1/count(*) as page_rank_score FROM dataset.all_nodes)
    	"""
    job = client.query(q1)
    job.result()

    for i in range(20):
        print("Step %d..." % (i + 1))

        q2 = """
            UPDATE dataset.pageranks as P
            SET P.page_rank_score = (WITH outgoing AS (SELECT g.src, avg(p.page_rank_score)/count(*) AS avg_pr
                                                    FROM dataset.GRAPH AS g inner join dataset.pageranks AS p
                                                    ON g.src = p.twitter_username
                                                    GROUP BY g.src)
                                    SELECT CASE WHEN sum(O.avg_pr) IS NULL THEN 0 ELSE sum(O.avg_pr) END
                                    FROM dataset.GRAPH AS G, outgoing AS O
                                    WHERE G.dst = P.twitter_username AND G.src = O.src)
            WHERE TRUE
            """

        job = client.query(q2)
        job.result()

    q3 = """
        SELECT * FROM dataset.pageranks ORDER BY page_rank_score DESC LIMIT 100
        """
    job = client.query(q3)
    results = job.result()
    return list(results)


# Do not edit this function. This is for helping you develop your own
# iterative PageRank algorithm.
def bfs(client, start, n_iter):

    # You should replace dataset.bfs_graph with your dataset name and table
    # name.
    q1 = """
        CREATE TABLE IF NOT EXISTS dataset.bfs_graph (src string, dst string);
        """
    q2 = """
        INSERT INTO dataset.bfs_graph(src, dst) VALUES
        ('A', 'B'),
        ('A', 'E'),
        ('B', 'C'),
        ('C', 'D'),
        ('E', 'F'),
        ('F', 'D'),
        ('A', 'F'),
        ('B', 'E'),
        ('B', 'F'),
        ('A', 'G'),
        ('B', 'G'),
        ('F', 'G'),
        ('H', 'A'),
        ('G', 'H'),
        ('H', 'C'),
        ('H', 'D'),
        ('E', 'H'),
        ('F', 'H');
        """

    job = client.query(q1)
    results = job.result()
    job = client.query(q2)
    results = job.result()

    # You should replace dataset.distances with your dataset name and table
    # name.
    q3 = """
        CREATE OR REPLACE TABLE dataset.distances AS
        SELECT '{start}' as node, 0 as distance
        """.format(start=start)
    job = client.query(q3)
    # Result will be empty, but calling makes the code wait for the query to
    # complete
    job.result()

    for i in range(n_iter):
        print("Step %d..." % (i + 1))
        q1 = """
        INSERT INTO dataset.distances(node, distance)
        SELECT distinct dst, {next_distance}
        FROM dataset.bfs_graph
            WHERE src IN (
                SELECT node
                FROM dataset.distances
                WHERE distance = {curr_distance}
                )
            AND dst NOT IN (
                SELECT node
                FROM dataset.distances
                )
            """.format(
            curr_distance=i,
            next_distance=i + 1
        )
        job = client.query(q1)
        results = job.result()
        # print(results)


# Do not edit this function. You can use this function to see how to store
# tables using BigQuery.
def save_table():
    client = bigquery.Client()
    dataset_id = 'dataset'

    job_config = bigquery.QueryJobConfig()
    # Set use_legacy_sql to True to use legacy SQL syntax.
    job_config.use_legacy_sql = True
    # Set the destination table
    table_ref = client.dataset(dataset_id).table('test')
    job_config.destination = table_ref
    job_config.allow_large_results = True
    sql = """select * from [w4111-columbia.graph.tweets] limit 3"""

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))


@click.command()
@click.argument("PATHTOCRED", type=click.Path(exists=True))
def main(pathtocred):
    client = bigquery.Client.from_service_account_json(pathtocred)

    funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
    # funcs_to_test = [q5]
    for func in funcs_to_test:
        rows = func(client)
        print("\n====%s====" % func.__name__)
        print(rows)

    #bfs(client, 'A', 5)

if __name__ == "__main__":
    main()
