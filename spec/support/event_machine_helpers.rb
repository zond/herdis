module EventMachineHelpers
  def uses_event_machine
    around(:each) do |example|
      EM.synchrony do
        example.run
        EM.stop
      end
    end
  end
end
