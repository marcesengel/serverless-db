import pRetry = require('p-retry')
import PQueue from 'p-queue'

interface DatabaseAdapter<TConnection extends Connection> {
  createConnection(): TConnection | Promise<TConnection>;
  isFatalConnectionError(error: any): boolean;
  isTooManyConnectionsError(error: any): boolean;
  closeOnFatal?: boolean; // defaults to true
}
interface ServerlessConnection<TConnection extends Connection> {
  get<TResult>(callback: (connection: TConnection, attempt: number) => Promise<TResult>): Promise<TResult>;
}

interface Connection {
  close(): unknown;
}

const createServerlessConnection = <TConnection extends Connection>(
  databaseAdapter: DatabaseAdapter<TConnection>,
  retryOptions?: pRetry.Options
): ServerlessConnection<TConnection> => {
  const getConnectionQueue = new PQueue({ concurrency: 1 })
  let connection: TConnection | null = null

  return {
    get: (callback) => (
      getConnectionQueue.add(() => pRetry(async attempt => {
        if (connection === null) {
          connection = await databaseAdapter.createConnection()
        }
  
        const result = await callback(connection, attempt)
        return result
      }, {
        ...retryOptions,
        onFailedAttempt: async error => {
          if (connection === null) {
            throw new Error('Invalid state: Received an exception while connection === null.')
          }

          if (databaseAdapter.isFatalConnectionError(error)) {
            if (databaseAdapter.closeOnFatal || databaseAdapter.closeOnFatal === undefined) {
              await Promise.resolve(connection.close())
                .finally(() => { connection = null })
            }

            connection = null
          }

          if (retryOptions?.onFailedAttempt) {
            await retryOptions.onFailedAttempt(error)
          }

          if (!databaseAdapter.isTooManyConnectionsError(error)) {
            throw error
          }
        }
      }))
    )
  }
}
