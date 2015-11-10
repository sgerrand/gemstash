require "spec_helper"
require "thread"

describe Gemstash::Preload do
  let(:stubs) { Faraday::Adapter::Test::Stubs.new }
  let(:http_client) { Gemstash::HTTPClient.new(Faraday.new {|builder| builder.adapter(:test, stubs) }) }
  let(:latest_specs) do
    to_marshaled_gzipped_bytes([["latest_gem", "1.0.0", ""]])
  end
  let(:full_specs) do
    to_marshaled_gzipped_bytes([["latest_gem", "1.0.0", ""], ["other", "0.1.0", ""]])
  end

  describe Gemstash::Preload::GemSpecs do
    it "GemSpecs fetches the full specs by default" do
      stubs.get("specs.4.8.gz") do
        [200, { "CONTENT-TYPE" => "octet/stream" }, full_specs]
      end
      specs = Gemstash::Preload::GemSpecs.new(http_client).fetch
      gems = specs.to_a
      expect(gems).not_to be_empty
      expect(gems.first.to_s).to eq("latest_gem-1.0.0")
      expect(gems.last.to_s).to eq("other-0.1.0")
    end

    it "GemSpecs fetches the latest specs when requested" do
      stubs.get("latest_specs.4.8.gz") do
        [200, { "CONTENT-TYPE" => "octet/stream" }, latest_specs]
      end
      specs = Gemstash::Preload::GemSpecs.new(http_client, latest: true).fetch
      expect(specs.to_a.last.to_s).to eq("latest_gem-1.0.0")
    end
  end

  describe Gemstash::Preload::GemPreloader do
    let(:invoked_stubs) { Queue.new }
    let(:expected_stubs) { [] }

    def verify_stubbed_calls
      actual_stubs = []
      actual_stubs << invoked_stubs.pop until invoked_stubs.empty?
      expect(actual_stubs).to match_array(expected_stubs)
    end

    def prepare_stubs(*included_stubs)
      if included_stubs.include?(:latest_gem)
        expected_stubs << :latest_gem
        stubs.head("gems/latest_gem-1.0.0.gem") do
          invoked_stubs << :latest_gem
          [200, { "CONTENT-TYPE" => "octet/stream" }, "The latest gem"]
        end
      end

      if included_stubs.include?(:other)
        expected_stubs << :other
        stubs.head("gems/other-0.1.0.gem") do
          invoked_stubs << :other
          [200, { "CONTENT-TYPE" => "octet/stream" }, "The other gem"]
        end
      end

      expected_stubs << :specs
      stubs.get("specs.4.8.gz") do
        invoked_stubs << :specs
        [200, { "CONTENT-TYPE" => "octet/stream" }, full_specs]
      end
    end

    let(:preloader) { Gemstash::Preload::GemPreloader.new(http_client) }

    it "Preloads all the gems included in the specs file" do
      prepare_stubs(:latest_gem, :other)
      preloader.preload
      verify_stubbed_calls
    end

    it "Skips gems as requested" do
      prepare_stubs(:other)
      preloader.skip(1).preload
      verify_stubbed_calls
    end

    it "Loads as many gems as requested" do
      prepare_stubs(:latest_gem)
      preloader.limit(1).preload
      verify_stubbed_calls
    end

    it "Loads only the last gem when requested" do
      prepare_stubs(:other)
      preloader.skip(1).limit(1).preload
      verify_stubbed_calls
    end

    it "Loads no gem at all when the skip is larger than the size" do
      prepare_stubs
      preloader.skip(3).preload
      verify_stubbed_calls
    end

    it "Loads no gem and no specs at all when the limit is zero" do
      preloader.limit(0).preload
      verify_stubbed_calls
    end

    it "Loads in order when using only one thread" do
      prepare_stubs(:latest_gem, :other)
      preloader.threads(1).preload
      verify_stubbed_calls
    end

    it "supports non existing gems while processing" do
      prepare_stubs(:other)
      stubs.head("gems/latest_gem-1.0.0.gem") do
        [404, {}, nil]
      end
      preloader.preload
      verify_stubbed_calls
    end

    it "supports having errors while processing" do
      prepare_stubs(:other)
      stubs.head("gems/latest_gem-1.0.0.gem") do
        raise Faraday::ConnectionFailed, "Just beause"
      end
      preloader.preload
      verify_stubbed_calls
    end
  end

  def to_marshaled_gzipped_bytes(obj)
    buffer = StringIO.new
    gzip = Zlib::GzipWriter.new(buffer)
    gzip.write(Marshal.dump(obj))
    gzip.close
    buffer.string
  end
end
