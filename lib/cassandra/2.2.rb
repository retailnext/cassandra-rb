class Cassandra
  def self.VERSION
    "2.2"
  end
end

require "#{File.expand_path(File.dirname(__FILE__))}/../cassandra"
