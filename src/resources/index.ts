import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { tablesResourceHandler } from "./tables.js";
import { tableStructureResourceHandler } from "./schema.js";
import { schemasResourceHandler } from "./schemas.js";
import { indexesResourceHandler } from "./indexes.js";
import { proceduresResourceHandler, procedureDetailResourceHandler } from "./procedures.js";
import { createResourceSuccessResponse, createResourceErrorResponse } from "../utils/response-formatter.js";

// Export all resource handlers
export { tablesResourceHandler } from "./tables.js";
export { tableStructureResourceHandler } from "./schema.js";
export { schemasResourceHandler } from "./schemas.js";
export { indexesResourceHandler } from "./indexes.js";
export { proceduresResourceHandler, procedureDetailResourceHandler } from "./procedures.js";

/**
 * Register all resource handlers with the MCP server
 */
export function registerResources(server: McpServer): void {
  // Resource for listing all schemas
  server.resource("schemas", "db://schemas", schemasResourceHandler);

  // Allow listing tables within a specific schema
  server.resource(
    "tables_in_schema",
    new ResourceTemplate("db://schemas/{schemaName}/tables", { list: undefined }),
    tablesResourceHandler
  );

  // Resource for getting table structure within a specific database schema
  server.resource(
    "table_structure_in_schema",
    new ResourceTemplate("db://schemas/{schemaName}/tables/{tableName}", { list: undefined }),
    tableStructureResourceHandler
  );

  // Resource for getting indexes for a table within a specific database schema
  server.resource(
    "indexes_in_table",
    new ResourceTemplate("db://schemas/{schemaName}/tables/{tableName}/indexes", {
      list: undefined,
    }),
    indexesResourceHandler
  );

  // Resource for listing stored procedures within a schema
  server.resource(
    "procedures_in_schema",
    new ResourceTemplate("db://schemas/{schemaName}/procedures", { list: undefined }),
    proceduresResourceHandler
  );

  // Resource for getting procedure detail within a schema
  server.resource(
    "procedure_detail_in_schema",
    new ResourceTemplate("db://schemas/{schemaName}/procedures/{procedureName}", {
      list: undefined,
    }),
    procedureDetailResourceHandler
  );
}

/**
 * Dynamically register resources for all tables in public schema (similar to archived server)
 */
export async function registerDynamicResources(server: McpServer, connector: any): Promise<void> {
  try {
    // Get all tables in public schema using the connector interface
    const tables = await connector.getTables("public");

    // Auto-register public schema tables list
    server.resource(
      "public_tables",
      "db://schemas/public/tables",
      async (uri: URL) => {
        const responseData = {
          tables: tables,
          count: tables.length
        };
        return createResourceSuccessResponse(uri.href, responseData);
      }
    );
    
    // Register individual table structure resources
    tables.forEach((tableName: string) => {
      // Register table structure resource
      server.resource(
        `public_table_${tableName}_structure`,
        `db://schemas/public/tables/${tableName}`,
        async (uri: URL) => {
          try {
            const columns = await connector.getTableSchema(tableName, "public");
            
            const responseData = {
              tableName,
              schema: "public",
              columns: columns,
              count: columns.length
            };
            return createResourceSuccessResponse(uri.href, responseData);
          } catch (error) {
            return createResourceErrorResponse(
              uri.href,
              `Error retrieving table structure: ${(error as Error).message}`,
              "TABLE_STRUCTURE_ERROR"
            );
          }
        }
      );

      // Register table indexes resource
      server.resource(
        `public_table_${tableName}_indexes`,
        `db://schemas/public/tables/${tableName}/indexes`,
        async (uri: URL) => {
          try {
            const indexes = await connector.getTableIndexes(tableName, "public");
            
            const responseData = {
              tableName,
              schema: "public",
              indexes: indexes,
              count: indexes.length
            };
            return createResourceSuccessResponse(uri.href, responseData);
          } catch (error) {
            return createResourceErrorResponse(
              uri.href,
              `Error retrieving table indexes: ${(error as Error).message}`,
              "TABLE_INDEXES_ERROR"
            );
          }
        }
      );
    });

    // Get all procedures/functions in public schema
    const procedures = await connector.getStoredProcedures("public");

    // Auto-register public schema procedures list
    server.resource(
      "public_procedures",
      "db://schemas/public/procedures",
      async (uri: URL) => {
        const responseData = {
          procedures: procedures,
          count: procedures.length
        };
        return createResourceSuccessResponse(uri.href, responseData);
      }
    );

    // Register individual procedure detail resources
    procedures.forEach((procedureName: string) => {
      server.resource(
        `public_procedure_${procedureName}_detail`,
        `db://schemas/public/procedures/${procedureName}`,
        async (uri: URL) => {
          try {
            const details = await connector.getStoredProcedureDetail(procedureName, "public");
            
            const responseData = {
              procedureName,
              schema: "public",
              details: details
            };
            return createResourceSuccessResponse(uri.href, responseData);
          } catch (error) {
            return createResourceErrorResponse(
              uri.href,
              `Error retrieving procedure details: ${(error as Error).message}`,
              "PROCEDURE_DETAIL_ERROR"
            );
          }
        }
      );
    });

    console.error(`Dynamically registered ${tables.length} table resources and ${procedures.length} procedure resources for public schema`);
    
  } catch (error) {
    console.error("Error during dynamic resource registration:", error);
  }
}
