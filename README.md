# RapidLMDB

# ⚠️ NOTICE: RapidLMDB is no longer being actively maintained. You are NOT encouraged to use this codebase on new projects.

If you find RapidLMDB appealing, please use QuickLMDB for future projects. QuickLMDB can be found [here](https://github.com/tannerdsilva/QuickLMDB).

WHY: There were two decisions that were baked into RapidLMDB at its inception that have limited its ability to grow.

- The decision to mirror release tags of this project with release tags of the LMDB core limited my ability to ship major updates
	
- The technical design around serialization greatly erodes many of the efficiencies that LMDB offers.
	
	- Primarily, the choice to encode numbers in their native bits, instead of using string-based encoding. This limits portability, and also makes it difficult to leverage LMDB's built in sorting features.