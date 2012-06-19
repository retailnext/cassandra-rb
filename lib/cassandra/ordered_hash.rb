require 'delegate'

# OrderedHash is namespaced to prevent conflicts with other implementations
class Cassandra
    class OrderedHashInt < Delegator #:nodoc:
      def initialize(*args, &block)
        @tuples = []
        @is_hash = false
        super(@hash = {})
      end

      def self.[](*args)
        ordered_hash = new

        if (args.length == 1 && args.first.is_a?(Array))
          args.first.each do |key_value_pair|
            next unless (key_value_pair.is_a?(Array))
            ordered_hash[key_value_pair[0]] = key_value_pair[1]
          end

          return ordered_hash
        end

        unless (args.size % 2 == 0)
          raise ArgumentError.new("odd number of arguments for Hash")
        end

        args.each_with_index do |val, ind|
          next if (ind % 2 != 0)
          ordered_hash[val] = args[ind + 1]
        end

        ordered_hash
      end

      def dup
        copy = self.class.new
        copy.send(:initialize_copy, self)
        copy
      end

      def initialize_copy(other)
        @tuples = other.to_a.dup
        @is_hash = false
      end

      def __getobj__
        unless @is_hash
          @hash = Hash[@tuples]
          @is_hash = true
        end

        return @hash
      end

      def __setobj__(obj)
        @hash = obj
      end

      def []=(key, value)
        @tuples << [key, value]
        @hash[key] = value if @is_hash
      end

      def delete(key)
        super if @is_hash

        if i = @tuples.index {|t| t.first == key }
          @tuples.delete_at(i).last
        end
      end

      def delete_if(&block)
        super if @is_hash

        @tuples.delete_if {|t| block.call(*t) }
        self
      end

      def reject!(&block)
        super if @is_hash

        @tuples.reject! {|t| block.call(*t) }
        self
      end

      def reject(&block)
        copy = dup
        copy.reject!(&block)
        copy
      end

      def keys
        @tuples.map(&:first)
      end

      def values
        @tuples.map(&:last)
      end

      def to_hash
        return self
      end

      def to_a
        @tuples
      end

      def each_key
        @tuples.each {|tuple| yield(tuple.first) }
      end

      def each_value
        @tuples.each {|tuple| yield(tuple.last) }
      end

      def each
        @tuples.each {|tuple| yield(tuple) }
      end

      alias_method :each_pair, :each

      def clear
        @tuples.clear
        super if @is_hash
        self
      end

      def shift
        t = @tuples.shift
        __getobj__.delete(t.first) if @is_hash
        t
      end

      def merge!(other_hash)
        other_hash.each {|k,v| self[k] = v }
        self
      end

      def merge(other_hash)
        dup.merge!(other_hash)
      end

      # When replacing with another hash, the initial order of our keys must come from the other hash -ordered or not.
      def replace(other)
        @tuples = other.to_a
        self
      end

      def reverse
        OrderedHashInt[self.to_a.reverse]
      end
    end

  class OrderedHash < OrderedHashInt #:nodoc:
    def initialize(*args, &block)
      @timestamps = OrderedHashInt.new
      super
    end

    def initialize_copy(other)
      @timestamps = other.timestamps
      super
    end

    def []=(key, value, timestamp = nil)
      @timestamps[key] = timestamp
      super(key, value)
    end

    def delete(key)
      @timestamps.delete(key)
      super
    end

    def delete_if(&block)
      @timestamps.delete_if(&block)
      super
    end

    def reject!(&block)
      @timestamps.reject!(&block)
      super
    end

    def timestamps
      @timestamps.dup
    end

    def clear
      @timestamps.clear
      super
    end

    def shift
      k, v = super
      @timestamps.delete(k)
      [k, v]
    end

    def replace(other)
      @timestamps = other.timestamps
      super
    end

    def inspect
      "#<OrderedHash #{super}\n#{@timestamps.inspect}>"
    end
  end
end
