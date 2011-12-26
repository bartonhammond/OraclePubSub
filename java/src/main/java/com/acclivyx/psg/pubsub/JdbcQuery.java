package com.acclivyx.psg.pubsub;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JdbcQuery {
	protected Connection connection = null;

	public JdbcQuery() throws Exception {

		try {

			Properties properties = getPropertiesFromClasspath("jdbc.properties");
			// Create a connection to the database
			String serverName = properties.getProperty("serverName");
			String portNumber = properties.getProperty("portNumber");
			String username = properties.getProperty("username");
			String password = properties.getProperty("password");

			// Load the JDBC driver
			String driverName = "oracle.jdbc.driver.OracleDriver";
			Class.forName(driverName);

			String url = "jdbc:oracle:thin:@" + serverName + ":" + portNumber;

			connection = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;

		}
	}

	private Properties getPropertiesFromClasspath(String propFileName)
			throws IOException {
		// loading xmlProfileGen.properties from the classpath
		Properties props = new Properties();
		InputStream inputStream = this.getClass().getClassLoader()
				.getResourceAsStream(propFileName);

		if (inputStream == null) {
			throw new FileNotFoundException("property file '" + propFileName
					+ "' not found in the classpath");
		}

		props.load(inputStream);

		return props;
	}

	public JSONArray performQuery(String query) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// Get the fetch size of a statement
			stmt = connection.createStatement();

			// Create a result set
			rs = stmt.executeQuery(query);

			return convertResultSetToJSON(rs);
		} finally {
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
		}

	}

	public JSONArray performRefCursor(String storedProc) throws Exception {
		JSONObject json = new JSONObject(storedProc);
		JSONArray args = json.getJSONArray("args");
		JSONArray arg_types = json.getJSONArray("arg_types");

		CallableStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareCall(json.getString("sp"));
			for (int i = 0; i < arg_types.length(); i++) {
				int arg_type = arg_types.getInt(i);
				if (arg_type == java.sql.Types.BIGINT) {
					stmt.setInt(i + 1, args.getInt(i));
				} else if (arg_type == java.sql.Types.BOOLEAN) {
					stmt.setBoolean(i + 1, args.getBoolean(i));
				} else if (arg_type == java.sql.Types.BLOB) {
					Blob blob = connection.createBlob();
					blob.setBytes(1, args.getString(i).getBytes());
					stmt.setBlob(i + 1, blob);
				} else if (arg_type == java.sql.Types.DOUBLE) {
					stmt.setDouble(i + 1, args.getDouble(i));
				} else if (arg_type == java.sql.Types.INTEGER) {
					stmt.setInt(i + 1, args.getInt(i));
				} else if (arg_type == java.sql.Types.NVARCHAR) {
					stmt.setNString(i + 1, args.getString(i));
				} else if (arg_type == java.sql.Types.VARCHAR) {
					stmt.setString(i + 1, args.getString(i));
				} else if (arg_type == java.sql.Types.TINYINT) {
					stmt.setInt(i + 1, args.getInt(i));
				} else if (arg_type == java.sql.Types.SMALLINT) {
					stmt.setShort(i + 1, (short) args.getInt(i));
				} else if (arg_type == java.sql.Types.DATE) {
					DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
					stmt.setDate(i + 1, new Date(df.parse(args.getString(i))
							.getTime()));
				} else {
					throw new Exception("invalid arg type: " + arg_type);
				}
			}

			stmt.registerOutParameter(json.getInt("rs"), OracleTypes.CURSOR); // REF
																				// CURSOR
			stmt.execute();
			rs = ((OracleCallableStatement) stmt).getCursor(2);
			return convertResultSetToJSON(rs);
		} finally {
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
		}
	}

	public JSONArray convertResultSetToJSON(java.sql.ResultSet rs)
			throws SQLException, JSONException {

		JSONArray json = new JSONArray();

		try {

			java.sql.ResultSetMetaData rsmd = rs.getMetaData();

			while (rs.next()) {
				int numColumns = rsmd.getColumnCount();
				JSONObject obj = new JSONObject();

				for (int i = 1; i < numColumns + 1; i++) {

					String column_name = rsmd.getColumnName(i);

					if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
						obj.put(column_name, rs.getArray(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
						obj.put(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
						obj.put(column_name, rs.getBoolean(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
						obj.put(column_name, rs.getBlob(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
						obj.put(column_name, rs.getDouble(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
						obj.put(column_name, rs.getFloat(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
						obj.put(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
						obj.put(column_name, rs.getNString(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
						obj.put(column_name, rs.getString(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
						obj.put(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
						obj.put(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
						obj.put(column_name, rs.getDate(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
						obj.put(column_name, rs.getTimestamp(column_name));
					} else {
						obj.put(column_name, rs.getObject(column_name));
					}

				}// end foreach
				json.put(obj);

			}// end while

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}

		return json;
	}
}
