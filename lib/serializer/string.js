var kafkaStringSerializer = function() {

}

kafkaStringSerializer.prototype.deserialize = function(buffer) {
  return buffer.toString();
}

kafkaStringSerializer.prototype.serialize = function(string) {
  return string;
}

module.exports = kafkaStringSerializer;
