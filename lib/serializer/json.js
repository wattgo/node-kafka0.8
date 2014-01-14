var kafkaJsonSerializer = function() {

}

kafkaJsonSerializer.prototype.deserialize = function(data) {
  return JSON.parse(data);
}

kafkaJsonSerializer.prototype.serialize = function(json) {
  return JSON.stringify(json);
}

module.exports = kafkaJsonSerializer;
