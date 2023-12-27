// Import AWS SDK for JavaScript
import AWS from 'aws-sdk';

// Initialize the Athena client
const athena = new AWS.Athena();

export async function handler(event) {
  const params = {
    QueryString: `SELECT * FROM "retail_entmaster1"."user-info" limit 10;`,
    ResultConfiguration: {
      OutputLocation: 's3://kamrul-athena-output/lambda-query/', // Updated output location
    },
    QueryExecutionContext: {
      Database: 'retail_entmaster1'
    }
  };

  try {
    const startQueryExecutionResponse = await athena.startQueryExecution(params).promise();
    const queryExecutionId = startQueryExecutionResponse.QueryExecutionId;

    // Wait for the query to complete
    let queryStatus = 'RUNNING';
    while (queryStatus === 'RUNNING') {
      const queryExecution = await athena.getQueryExecution({ QueryExecutionId: queryExecutionId }).promise();
      queryStatus = queryExecution.QueryExecution.Status.State;

      if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
        throw new Error('Query failed or was cancelled');
      }

      // Wait a bit before checking the status again
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // Fetch the query results
    const results = await athena.getQueryResults({ QueryExecutionId: queryExecutionId }).promise();

    // Process and log each row
    results.ResultSet.Rows.forEach(row => {
      console.log('Row:', row.Data.map(data => data.VarCharValue).join(', '));
    });

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Query executed and processed successfully',
        queryExecutionId: queryExecutionId
      }),
    };
  } catch (error) {
    console.error('Error executing query:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: 'Error executing query',
        error: error.message
      }),
    };
  }
}




// ---------
// console.log('Loading function');

// export const handler = async (event, context) => {
//     //console.log('Received event:', JSON.stringify(event, null, 2));
//     console.log('value1 =', event.start);
//     return event.key1;  // Echo back the first key value
//     // throw new Error('Something went wrong');
// };

