require "faraday"
require "parallel"
require "zlib"

#:nodoc:
module Gemstash
  #:nodoc:
  module Preload
    #:nodoc:
    class GemPreloader
      def initialize(http_client, latest: false)
        @http_client = http_client
        @threads = 20
        @skip = 0
        @specs = GemSpecs.new(http_client, latest: latest)
      end

      def threads(size)
        @threads = size
        self
      end

      def limit(size)
        @limit = size
        self
      end

      def skip(size)
        @skip = size
        self
      end

      def preload
        return if @limit && @limit <= 0
        return if gems.empty?

        Parallel.each(gems, in_threads: @threads, progress: "Preloading gems") do |gem_name|
          begin
            @http_client.head("gems/#{gem_name}.gem")
          rescue
            STDERR.puts "\nError while processing gem: #{gem_name}"
          end
        end
      end

    private

      def gems
        @gems ||= begin
          gem_specs = @specs.fetch.to_a
          range_end = (@limit || gem_specs.size) + @skip
          # Result is nil if @skip is > size
          gem_specs[@skip...range_end] || []
        end
      end
    end

    #:nodoc:
    class GemSpecs
      include Enumerable

      def initialize(http_client, latest: false)
        @http_client = http_client
        @specs_file = "specs.4.8.gz" unless latest
        @specs_file ||= "latest_specs.4.8.gz"
      end

      def fetch
        reader = Zlib::GzipReader.new(
          StringIO.new(@http_client.get(@specs_file)))
        @specs = Marshal.load(reader.read)
        self
      end

      def each(&block)
        @specs.each do |gem|
          yield GemName.new(gem)
        end
      end
    end

    #:nodoc:
    class GemName
      def initialize(gem)
        (@name, @version, _ignored) = gem
      end

      def to_s
        "#{@name}-#{@version}"
      end
    end
  end
end
