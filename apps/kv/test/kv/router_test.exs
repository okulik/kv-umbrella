
defmodule KV.RouterTest do
  use ExUnit.Case, async: true
  @moduletag :distributed

  test "route requests across nodes" do
    assert KV.Router.route("hello_bucket", Kernel, :node, []) == :"foo@surfer"
    assert KV.Router.route("world_bucket", Kernel, :node, []) == :"bar@surfer"
  end

  test "raises on unknown entries" do
    assert_raise RuntimeError, ~r/could not find entry/, fn ->
      KV.Router.route(<<0>>, Kernel, :node, [])
    end
  end
end