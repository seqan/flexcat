// ==========================================================================
// Author: Benjamin Menkuec <benjamin@menkuec.de>
// ==========================================================================


#pragma once

template<template<typename> class TRead, typename TSeq, typename TProgramParams, typename TInputFileStreams>
struct ReadReader
{
public:
    using itemType = std::vector<TRead<TSeq>>;
private:
    TInputFileStreams& _inputFileStreams;
    const TProgramParams& _programParams;
    unsigned int _numReads;

public:
    ReadReader(TInputFileStreams& inputFileStreams, const TProgramParams& programParams)
        : _inputFileStreams(inputFileStreams), _programParams(programParams), _numReads(0) {};
    bool operator()(std::unique_ptr<std::vector<TRead<TSeq>>>& item)
    {
        try {
            readReads(*item, _programParams.records, _inputFileStreams);
        }
        catch (std::exception& e) {
            std::cout << "exception while reading :" << e.what() << " after read " << _numReads << std::endl;
            throw(e);
        }
        loadMultiplex(*item, _programParams.records, _inputFileStreams.fileStreamMultiplex);
        _numReads += item->size();
        if (item->empty() || _numReads >= _programParams.firstReads)    // no more reads available or maximum read number reached -> dont do further reads
            return false;
        return true;
    }
};


