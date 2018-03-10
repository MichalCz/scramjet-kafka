const {Client} = require("kafka-node");

module.exports = {
    /**
     * Creates a new client or passes the given one
     *
     * @private
     * @param  {Client|ZKConnectionOptions} conn connection
     * @return {Client}
     */
    getConnection(conn) {
        if (conn instanceof Client) {
            return conn;
        } else {
            return new Client(conn);
        }
    }
};
